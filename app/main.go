package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Server Started")

	for {
		// Accept a connection
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		// Handle each client in a separate goroutine
		go handleClient(conn)
	}
}

// Processing commands from single client connection
func handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		// Reading line from the connection
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				break
			}
			fmt.Println("Error reading from connection:", err.Error())
			break
		}

		// Send PONG response regardless of input FOR NOW
		// TODO: IN future, parse the command and respond accordingly
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
			break
		}
	}
}
