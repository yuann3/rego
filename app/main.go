package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var (
	replicaConns    []net.Conn
	connMu          sync.RWMutex
	commandRegistry *command.Registry
)

func main() {
	dirFlag := flag.String("dir", "", "Directory where RDB files are stored")
	dbFilenameFlag := flag.String("dbfilename", "", "Name of the RDB file")
	portFlag := flag.Int("port", 6379, "Port to listen on")
	replicaofFlag := flag.String("replicaof", "", "Master host and port (e.g., 'localhost 6379')")
	flag.Parse()

	if *portFlag < 1 || *portFlag > 65535 {
		fmt.Println("Error: Port number must be between 1 and 65535")
		os.Exit(1)
	}

	if err := command.InitConfig(*dirFlag, *dbFilenameFlag, *replicaofFlag); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	commandRegistry = command.NewRegistry()
	command.InitRegistry(commandRegistry)

	config := command.GetServerConfig()
	if config.IsReplica {
		go func() {
			if err := connectToMaster(config.MasterHost, config.MasterPort, *portFlag, commandRegistry); err != nil {
				fmt.Printf("Error connecting to master: %v\n", err)
			}
		}()
	}

	rdbPath := filepath.Join(config.Dir, config.DBFilename)
	if _, err := os.Stat(rdbPath); err == nil {
		if err := rdb.Parse(rdbPath, command.GetStore()); err != nil {
			fmt.Printf("Warning: Failed to load RDB file: %v\n", err)
		} else {
			fmt.Printf("Loaded RDB file: %s\n", rdbPath)
		}
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *portFlag))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", *portFlag)
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Server Started")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn, commandRegistry)
	}
}

func handleClient(conn net.Conn, registry *command.Registry) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	clientID := conn.RemoteAddr().String()

	for {
		respObj, err := resp.Parse(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error parsing command:", err.Error())
			}
			break
		}

		response, extraBytes := processCommand(respObj, registry, conn, clientID)

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
	}
}

func processCommand(respObj resp.RESP, registry *command.Registry, conn net.Conn, clientID string) (resp.RESP, []byte) {
	if respObj.Type != resp.Array {
		return resp.NewError("ERR invalid command format"), nil
	}

	if len(respObj.Array) == 0 {
		return resp.NewError("ERR empty command"), nil
	}

	cmdNameResp := respObj.Array[0]
	if cmdNameResp.Type != resp.BulkString {
		return resp.NewError("ERR command must be a bulk string"), nil
	}

	cmdName := strings.ToUpper(cmdNameResp.String)
	handler, exists := registry.Get(cmdName)
	if !exists {
		return resp.NewError(fmt.Sprintf("ERR unknown command '%s'", cmdName)), nil
	}

	args := respObj.Array[1:]
	response, extraBytes := handler(args)

	if cmdName == "PSYNC" {
		connMu.Lock()
		replicaConns = append(replicaConns, conn)
		registry.UpdateReplicaAck(conn, 0)
		connMu.Unlock()
	}

	if registry.IsWriteCommand(cmdName) && !command.GetServerConfig().IsReplica {
		propagateCommand(respObj, registry, clientID)
	}

	return response, extraBytes
}

func propagateCommand(cmd resp.RESP, registry *command.Registry, clientID string) {
	connMu.RLock()
	conns := make([]net.Conn, len(replicaConns))
	copy(conns, replicaConns)
	connMu.RUnlock()

	var toRemove []int
	cmdBytes := []byte(cmd.Marshal())
	cmdSize := len(cmdBytes)

	command.IncrementMasterReplOffset(cmdSize)
	currentOffset := command.GetMasterReplOffset()
	registry.SetClientOffset(clientID, currentOffset)

	for i, conn := range conns {
		_, err := conn.Write(cmdBytes)
		if err != nil {
			fmt.Printf("Error propagating command to replica: %v\n", err)
			toRemove = append(toRemove, i)
		}
	}

	if len(toRemove) > 0 {
		connMu.Lock()
		for i := len(toRemove) - 1; i >= 0; i-- {
			idx := toRemove[i]
			registry.RemoveReplica(replicaConns[idx])
			replicaConns[idx].Close()
			replicaConns = slices.Delete(replicaConns, idx, idx+1)
		}
		connMu.Unlock()
	}
}

func connectToMaster(masterHost string, masterPort int, replicaPort int, registry *command.Registry) error {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, fmt.Sprintf("%d", masterPort)))
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer conn.Close()

	pingCmd := resp.NewArray([]resp.RESP{resp.NewBulkString("PING")})
	if _, err := conn.Write([]byte(pingCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send PING to master: %w", err)
	}

	reader := bufio.NewReader(conn)
	respObj, err := resp.Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response: %w", err)
	}
	if respObj.Type != resp.SimpleString || respObj.String != "PONG" {
		return fmt.Errorf("unexpected response to PING: %v", respObj)
	}
	fmt.Println("Received PONG from master")

	portCmd := resp.NewArray([]resp.RESP{
		resp.NewBulkString("REPLCONF"),
		resp.NewBulkString("listening-port"),
		resp.NewBulkString(strconv.Itoa(replicaPort)),
	})
	if _, err := conn.Write([]byte(portCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port to master: %w", err)
	}

	respObj, err = resp.Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response to REPLCONF listening-port: %w", err)
	}
	if respObj.Type != resp.SimpleString || respObj.String != "OK" {
		return fmt.Errorf("unexpected response to REPLCONF listening-port: %v", respObj)
	}

	capaCmd := resp.NewArray([]resp.RESP{
		resp.NewBulkString("REPLCONF"),
		resp.NewBulkString("capa"),
		resp.NewBulkString("psync2"),
	})
	if _, err := conn.Write([]byte(capaCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send REPLCONF capa to master: %w", err)
	}

	respObj, err = resp.Parse(reader)
	if err != nil {
		return fmt.Errorf("failed to read master response to REPLCONF capa: %w", err)
	}
	if respObj.Type != resp.SimpleString || respObj.String != "OK" {
		return fmt.Errorf("unexpected response to REPLCONF capa: %v", respObj)
	}

	psyncCmd := resp.NewArray([]resp.RESP{
		resp.NewBulkString("PSYNC"),
		resp.NewBulkString("?"),
		resp.NewBulkString("-1"),
	})
	if _, err := conn.Write([]byte(psyncCmd.Marshal())); err != nil {
		return fmt.Errorf("failed to send PSYNC to master: %w", err)
	}

	respObj, err = resp.Parse(reader)
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

	fmt.Println("Handshake completed successfully")

	offset := 0

	for {
		respObj, err := resp.Parse(reader)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Master connection closed")
				return nil
			}
			fmt.Printf("Error parsing propagated command: %v\n", err)
			continue
		}

		cmdBytes := respObj.Marshal()
		cmdSize := len(cmdBytes)

		if isReplconfGetack(respObj) {
			ackCmd := resp.NewArray([]resp.RESP{
				resp.NewBulkString("REPLCONF"),
				resp.NewBulkString("ACK"),
				resp.NewBulkString(strconv.Itoa(offset)),
			})

			if _, err := conn.Write([]byte(ackCmd.Marshal())); err != nil {
				fmt.Printf("Error sending ACK to master: %v\n", err)
			}
		} else if isReplconfAck(respObj) {
			if len(respObj.Array) >= 3 && respObj.Array[2].Type == resp.BulkString {
				ackOffset, err := strconv.Atoi(respObj.Array[2].String)
				if err == nil {
					registry.UpdateReplicaAck(conn, ackOffset)
				}
			}
		} else if respObj.Type == resp.Array && len(respObj.Array) > 0 {
			cmdNameResp := respObj.Array[0]
			if cmdNameResp.Type == resp.BulkString {
				cmdName := strings.ToUpper(cmdNameResp.String)
				handler, exists := registry.Get(cmdName)
				if exists {
					args := respObj.Array[1:]
					handler(args)
				} else {
					fmt.Printf("Unknown command from master: %s\n", cmdName)
				}
			}
		}

		offset += cmdSize
	}
}

func isReplconfGetack(cmd resp.RESP) bool {
	return cmd.Type == resp.Array &&
		len(cmd.Array) == 3 &&
		cmd.Array[0].Type == resp.BulkString &&
		strings.ToUpper(cmd.Array[0].String) == "REPLCONF" &&
		cmd.Array[1].Type == resp.BulkString &&
		strings.ToUpper(cmd.Array[1].String) == "GETACK" &&
		cmd.Array[2].Type == resp.BulkString &&
		cmd.Array[2].String == "*"
}

func isReplconfAck(cmd resp.RESP) bool {
	return cmd.Type == resp.Array &&
		len(cmd.Array) >= 3 &&
		cmd.Array[0].Type == resp.BulkString &&
		strings.ToUpper(cmd.Array[0].String) == "REPLCONF" &&
		cmd.Array[1].Type == resp.BulkString &&
		strings.ToUpper(cmd.Array[1].String) == "ACK"
}
