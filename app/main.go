package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Server Started")

	commandRegistry := command.NewRegistry()

	for {
		// Accept a connection
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		// Handle each client in a separate goroutine
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

		response := processCommand(respObj, registry)

		// Write the response
		_, err = conn.Write([]byte(response.Marshal()))
		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
			break
		}
	}
}

func processCommand(respObj resp.RESP, registry *command.Registry) resp.RESP {
	if respObj.Type != resp.Array {
		return resp.NewError("ERR invalid command format")
	}

	if len(respObj.Array) == 0 {
		return resp.NewError("ERR empty command")
	}

	// The first element is the command name
	cmdNameResp := respObj.Array[0]
	if cmdNameResp.Type != resp.BulkString {
		return resp.NewError("ERR command must be a bulk string")
	}

	// Get the command handler
	cmdName := strings.ToUpper(cmdNameResp.String)
	handler, exists := registry.Get(cmdName)
	if !exists {
		return resp.NewError(fmt.Sprintf("ERR unknown command '%s'", cmdName))
	}

	// Execute the command with arguments
	args := respObj.Array[1:]
	return handler(args)
}
