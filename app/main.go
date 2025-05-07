package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type ClientState struct {
	InTransaction  bool
	QueuedCommands []RESP
	mu             sync.RWMutex
}

var (
	clientStates      = make(map[net.Conn]*ClientState)
	clientStatesMutex sync.RWMutex
)

func getClientState(conn net.Conn) *ClientState {
	clientStatesMutex.RLock()
	state, exists := clientStates[conn]
	clientStatesMutex.RUnlock()

	if !exists {
		clientStatesMutex.Lock()
		state = &ClientState{}
		clientStates[conn] = state
		clientStatesMutex.Unlock()
	}

	return state
}

func removeClientState(conn net.Conn) {
	clientStatesMutex.Lock()
	delete(clientStates, conn)
	clientStatesMutex.Unlock()
}
func main() {
	fmt.Println("Starting Redis server...")

	dirFlag := flag.String("dir", ".", "Directory where RDB files are stored")
	dbFilenameFlag := flag.String("dbfilename", "dump.rdb", "Name of the RDB file")
	portFlag := flag.Int("port", 6379, "Port to listen on")
	replicaofFlag := flag.String("replicaof", "", "Master host and port (e.g., 'localhost 6379')")
	flag.Parse()

	if *portFlag < 1 || *portFlag > 65535 {
		fmt.Println("Error: Port number must be between 1 and 65535")
		os.Exit(1)
	}

	if err := InitConfig(*dirFlag, *dbFilenameFlag, *replicaofFlag); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	config := GetServerConfig()
	registry := NewRegistry()

	rdbPath := filepath.Join(config.Dir, config.DBFilename)
	if _, err := os.Stat(rdbPath); err == nil {
		fmt.Printf("Loading RDB file: %s\n", rdbPath)
		if err := ParseRDB(rdbPath, GetStore()); err != nil {
			fmt.Printf("Warning: Failed to load RDB file: %v\n", err)
		} else {
			keys := GetStore().Keys()
			fmt.Printf("Successfully loaded %d keys from RDB file\n", len(keys))
			for _, key := range keys {
				value, _ := GetStore().Get(key)
				fmt.Printf("  %s -> %s\n", key, value)
			}
		}
	}

	if config.IsReplica {
		go func() {
			if err := connectToMaster(config.MasterHost, config.MasterPort, *portFlag, registry); err != nil {
				fmt.Printf("Error connecting to master: %v\n", err)
			}
		}()
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *portFlag))
	if err != nil {
		fmt.Printf("Failed to bind to port %d: %v\n", *portFlag, err)
		os.Exit(1)
	}
	defer l.Close()

	fmt.Printf("Server started on port %d\n", *portFlag)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}

		go handleClient(conn, registry)
	}
}

func handleClient(conn net.Conn, registry *Registry) {
	defer conn.Close()
	defer removeClientState(conn)
	reader := bufio.NewReader(conn)

	for {
		respObj, err := Parse(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error parsing command:", err.Error())
			}
			break
		}

		response, extraBytes := processCommand(respObj, registry, conn)

		if _, err := conn.Write([]byte(response.Marshal())); err != nil {
			fmt.Println("Error writing to connection:", err.Error())
			break
		}

		if len(extraBytes) > 0 {
			if _, err := conn.Write(extraBytes); err != nil {
				fmt.Println("Error writing extra bytes to connection:", err.Error())
				break
			}
		}

		if registry.IsWriteCommand(respObj.Array[0].String) && !GetServerConfig().IsReplica {
			bytesWritten := int64(len(response.Marshal()))
			if len(extraBytes) > 0 {
				bytesWritten += int64(len(extraBytes))
			}
			IncrementOffset(bytesWritten)
		}
	}
}

func processCommand(respObj RESP, registry *Registry, conn net.Conn) (RESP, []byte) {
	if respObj.Type != Array {
		return NewError("ERR invalid command format"), nil
	}

	if len(respObj.Array) == 0 {
		return NewError("ERR empty command"), nil
	}

	cmdNameResp := respObj.Array[0]
	if cmdNameResp.Type != BulkString {
		return NewError("ERR command must be a bulk string"), nil
	}

	cmdName := strings.ToUpper(cmdNameResp.String)

	state := getClientState(conn)
	state.mu.RLock()
	InTransaction := state.InTransaction
	state.mu.RUnlock()

	if InTransaction && cmdName != "EXEC" && cmdName != "MULTI" && cmdName != "DISCARD" {
		state.mu.Lock()
		state.QueuedCommands = append(state.QueuedCommands, respObj)
		state.mu.Unlock()
		return NewSimpleString("QUEUED"), nil
	}

	handler, exists := registry.Get(cmdName)
	if !exists {
		return NewError(fmt.Sprintf("ERR unknown command '%s'", cmdName)), nil
	}

	args := respObj.Array[1:]
	response, extraBytes := handler(args, conn)

	if cmdName == "PSYNC" {
		AddReplica(conn)
	}

	if cmdName == "REPLCONF" && len(args) >= 2 &&
		strings.ToUpper(args[0].String) == "ACK" {
		offset, err := strconv.ParseInt(args[1].String, 10, 64)
		if err == nil {
			UpdateReplicaOffset(conn, offset)
		}
	}

	if registry.IsWriteCommand(cmdName) && !GetServerConfig().IsReplica {
		bytesWritten := int64(len(response.Marshal()))
		if len(extraBytes) > 0 {
			bytesWritten += int64(len(extraBytes))
		}
		IncrementOffset(bytesWritten)

		fmt.Printf("Propagating %s command to replicas\n", cmdName)
		propagateCommand(respObj)
	}

	return response, extraBytes
}

func propagateCommand(cmd RESP) {
	conns := GetReplicaConnections()
	if len(conns) == 0 {
		return
	}

	cmdBytes := []byte(cmd.Marshal())

	var toRemove []net.Conn
	for _, conn := range conns {
		_, err := conn.Write(cmdBytes)
		if err != nil {
			fmt.Printf("Error propagating command to replica: %v\n", err)
			toRemove = append(toRemove, conn)
		} else {
			fmt.Printf("Successfully propagated command to replica: %s\n", cmd.Array[0].String)
		}
	}

	for _, conn := range toRemove {
		RemoveReplica(conn)
		conn.Close()
	}
}

func connectToMaster(masterHost string, masterPort int, replicaPort int, registry *Registry) error {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, fmt.Sprintf("%d", masterPort)))
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer conn.Close()

	pingCmd := NewArray([]RESP{NewBulkString("PING")})
	if _, err := conn.Write([]byte(pingCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send PING to master: %w", err)
	}

	reader := bufio.NewReader(conn)
	respObj, err := Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response: %w", err)
	}
	if respObj.Type != SimpleString || respObj.String != "PONG" {
		return fmt.Errorf("unexpected response to PING: %v", respObj)
	}
	fmt.Println("Received PONG from master")

	portCmd := NewArray([]RESP{
		NewBulkString("REPLCONF"),
		NewBulkString("listening-port"),
		NewBulkString(strconv.Itoa(replicaPort)),
	})
	if _, err := conn.Write([]byte(portCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port to master: %w", err)
	}

	respObj, err = Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response to REPLCONF listening-port: %w", err)
	}
	if respObj.Type != SimpleString || respObj.String != "OK" {
		return fmt.Errorf("unexpected response to REPLCONF listening-port: %v", respObj)
	}

	capaCmd := NewArray([]RESP{
		NewBulkString("REPLCONF"),
		NewBulkString("capa"),
		NewBulkString("psync2"),
	})
	if _, err := conn.Write([]byte(capaCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send REPLCONF capa to master: %w", err)
	}

	respObj, err = Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response to REPLCONF capa: %w", err)
	}
	if respObj.Type != SimpleString || respObj.String != "OK" {
		return fmt.Errorf("unexpected response to REPLCONF capa: %v", respObj)
	}

	psyncCmd := NewArray([]RESP{
		NewBulkString("PSYNC"),
		NewBulkString("?"),
		NewBulkString("-1"),
	})
	if _, err := conn.Write([]byte(psyncCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send PSYNC to master: %w", err)
	}

	respObj, err = Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response to PSYNC: %w", err)
	}
	fmt.Println("Received response to PSYNC:", respObj.String)

	b, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read RDB marker: %w", err)
	}

	if b != '$' {
		return fmt.Errorf("expected '$', got '%c'", b)
	}

	sizeStr, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB size: %w", err)
	}

	sizeStr = strings.TrimSuffix(sizeStr, "\r\n")
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return fmt.Errorf("invalid RDB size: %w", err)
	}

	rdbBytes := make([]byte, size)
	if _, err := io.ReadFull(reader, rdbBytes); err != nil {
		return fmt.Errorf("failed to read RDB file: %w", err)
	}

	offsetMu.Lock()
	currentOffset = 0
	offsetMu.Unlock()

	fmt.Println("Handshake completed successfully")

	var commandHistory []int64

	for {
		respObj, err := Parse(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Master connection closed")
				return nil
			}
			fmt.Printf("Error parsing propagated command: %v\n", err)
			continue
		}

		if respObj.Type != Array || len(respObj.Array) == 0 {
			fmt.Println("Received invalid command format from master")
			continue
		}

		commandBytes := respObj.Marshal()
		bytesCount := int64(len(commandBytes))

		isGetAck := false
		if len(respObj.Array) >= 3 &&
			respObj.Array[0].Type == BulkString && strings.ToUpper(respObj.Array[0].String) == "REPLCONF" &&
			respObj.Array[1].Type == BulkString && strings.ToUpper(respObj.Array[1].String) == "GETACK" {
			isGetAck = true
		}

		if !isGetAck {
			cmdNameResp := respObj.Array[0]
			if cmdNameResp.Type != BulkString {
				fmt.Println("Command name is not a bulk string")
				continue
			}
			cmdName := strings.ToUpper(cmdNameResp.String)
			handler, exists := registry.Get(cmdName)
			if !exists {
				fmt.Printf("Unknown command from master: %s\n", cmdName)
				continue
			}
			args := respObj.Array[1:]
			handler(args, conn)

			commandHistory = append(commandHistory, bytesCount)
			fmt.Printf("Added %d bytes for %s command, history: %v\n",
				bytesCount, cmdName, commandHistory)
		} else {
			var totalBytes int64
			for _, size := range commandHistory {
				totalBytes += size
			}

			commandHistory = append(commandHistory, bytesCount)

			offsetMu.Lock()
			currentOffset = totalBytes
			offsetMu.Unlock()

			fmt.Printf("GETACK received, total bytes in history: %d, offset set to: %d\n",
				totalBytes, currentOffset)

			cmdNameResp := respObj.Array[0]
			cmdName := strings.ToUpper(cmdNameResp.String)
			handler, exists := registry.Get(cmdName)
			if !exists {
				fmt.Printf("Unknown command from master: %s\n", cmdName)
				continue
			}
			args := respObj.Array[1:]
			response, _ := handler(args, conn) // Fixed: Added conn argument

			if _, err := conn.Write([]byte(response.Marshal())); err != nil {
				fmt.Printf("Error sending response to master: %v\n", err)
			}
		}
	}
}
