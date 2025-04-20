package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
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

	config := command.GetServerConfig()
	if config.IsReplica {
		go func() {
			if err := connectToMaster(config.MasterHost, config.MasterPort, *portFlag); err != nil {
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

	commandRegistry := command.NewRegistry()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClient(conn, commandRegistry)
	}
}

// Processing commands from single client connection
func handleClient(conn net.Conn, registry *command.Registry) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		respObj, err := resp.Parse(reader)
		if err != nil {
			fmt.Println("Error parsing command:", err.Error())
			break
		}

		response, extraBytes := processCommand(respObj, registry)

		// Write the RESP response
		if _, err := conn.Write([]byte(response.Marshal())); err != nil {
			fmt.Println("Error writing to connection:", err.Error())
			break
		}

		// Write any extra bytes (e.g., RDB file)
		if len(extraBytes) > 0 {
			if _, err := conn.Write(extraBytes); err != nil {
				fmt.Println("Error writing extra bytes to connection:", err.Error())
				break
			}
		}
	}
}

func processCommand(respObj resp.RESP, registry *command.Registry) (resp.RESP, []byte) {
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
	return handler(args)
}

func connectToMaster(masterHost string, masterPort int, replicaPort int) error {
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

	fmt.Println("Handshake completed successfully")
	return nil
}
