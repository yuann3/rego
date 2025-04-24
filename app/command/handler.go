package command

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var emptyRDB = []byte{
	0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
	0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
	0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
	0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
	0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
	0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
}

type Handler func(args []resp.RESP, conn net.Conn) (resp.RESP, []byte)

type Registry struct {
	commands   map[string]Handler
	isWriteCmd map[string]bool
}

func NewRegistry() *Registry {
	r := &Registry{
		commands:   make(map[string]Handler),
		isWriteCmd: make(map[string]bool),
	}
	r.registerCommands()
	return r
}

func (r *Registry) registerCommands() {
	r.Register("PING", pingCommand, false)
	r.Register("ECHO", echoCommand, false)
	r.Register("SET", setCommand, true)
	r.Register("GET", getCommand, false)
	r.Register("CONFIG", configCommand, false)
	r.Register("KEYS", keysCommand, false)
	r.Register("INFO", infoCommand, false)
	r.Register("REPLCONF", replconfCommand, false)
	r.Register("PSYNC", psyncCommand, false)
	r.Register("WAIT", waitCommand, false)
}

func (r *Registry) Register(name string, handler Handler, isWrite bool) {
	name = strings.ToUpper(name)
	r.commands[name] = handler
	r.isWriteCmd[name] = isWrite
}

func (r *Registry) Get(name string) (Handler, bool) {
	handler, ok := r.commands[strings.ToUpper(name)]
	return handler, ok
}

func (r *Registry) IsWriteCommand(name string) bool {
	return r.isWriteCmd[strings.ToUpper(name)]
}

func pingCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	if len(args) == 0 {
		return resp.NewSimpleString("PONG"), nil
	}
	return resp.NewBulkString(args[0].String), nil
}

func echoCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	if len(args) == 0 {
		return resp.NewError("ERR wrong number of arguments for 'echo' command"), nil
	}
	return resp.NewBulkString(args[0].String), nil
}

func replconfCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	return resp.NewSimpleString("OK"), nil
}

func psyncCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	response := fmt.Sprintf("FULLRESYNC %s %d", masterReplID, masterReplOffset)

	rdbBytes := make([]byte, 0, len(emptyRDB)+16)
	rdbBytes = append(rdbBytes, '$')
	rdbBytes = append(rdbBytes, []byte(strconv.Itoa(len(emptyRDB)))...)
	rdbBytes = append(rdbBytes, '\r', '\n')
	rdbBytes = append(rdbBytes, emptyRDB...)

	return resp.NewSimpleString(response), rdbBytes
}

func waitCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	if len(args) != 2 {
		return resp.NewError("ERR wrong number of arguments for 'wait' command"), nil
	}

	numReplicas, err := strconv.Atoi(args[0].String)
	if err != nil || numReplicas < 0 {
		return resp.NewError("ERR value is not an integer or out of range"), nil
	}

	timeout, err := strconv.Atoi(args[1].String)
	if err != nil || timeout < 0 {
		return resp.NewError("ERR value is not an integer or out of range"), nil
	}

	clientOffset, err := getClientOffset(conn)
	if err != nil {
		return resp.NewInteger(0), nil
	}

	if numReplicas == 0 {
		return resp.NewInteger(0), nil
	}

	count := countAcknowledgingReplicas(clientOffset)
	if count >= numReplicas {
		return resp.NewInteger(count), nil
	}

	if timeout == 0 {
		for {
			waitForAcknowledgment()

			count = countAcknowledgingReplicas(clientOffset)
			if count >= numReplicas {
				return resp.NewInteger(count), nil
			}
		}
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				count = countAcknowledgingReplicas(clientOffset)
				return resp.NewInteger(count), nil
			}

			count = countAcknowledgingReplicas(clientOffset)
			if count >= numReplicas {
				return resp.NewInteger(count), nil
			}
		}
	}
}
