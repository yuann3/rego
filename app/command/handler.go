package command

import (
	"fmt"
	"strconv"
	"strings"

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

type Handler func(args []resp.RESP) (resp.RESP, []byte)

type Registry struct {
	commands map[string]Handler
}

func NewRegistry() *Registry {
	r := &Registry{
		commands: make(map[string]Handler),
	}
	r.registerCommands()
	return r
}

func (r *Registry) registerCommands() {
	r.Register("PING", pingCommand)
	r.Register("ECHO", echoCommand)
	r.Register("SET", setCommand)
	r.Register("GET", getCommand)
	r.Register("CONFIG", configCommand)
	r.Register("KEYS", keysCommand)
	r.Register("INFO", infoCommand)
	r.Register("REPLCONF", replconfCommand)
	r.Register("PSYNC", psyncCommand)
}

func (r *Registry) Register(name string, handler Handler) {
	r.commands[strings.ToUpper(name)] = handler
}

func (r *Registry) Get(name string) (Handler, bool) {
	handler, ok := r.commands[strings.ToUpper(name)]
	return handler, ok
}

func pingCommand(args []resp.RESP) (resp.RESP, []byte) {
	if len(args) == 0 {
		return resp.NewSimpleString("PONG"), nil
	}
	return resp.NewBulkString(args[0].String), nil
}

func echoCommand(args []resp.RESP) (resp.RESP, []byte) {
	if len(args) == 0 {
		return resp.NewError("ERR wrong number of arguments for 'echo' command"), nil
	}
	return resp.NewBulkString(args[0].String), nil
}

func replconfCommand(args []resp.RESP) (resp.RESP, []byte) {
	return resp.NewSimpleString("OK"), nil
}

func psyncCommand(args []resp.RESP) (resp.RESP, []byte) {
	response := fmt.Sprintf("FULLRESYNC %s %d", masterReplID, masterReplOffset)

	rdbBytes := make([]byte, 0, len(emptyRDB)+16)
	rdbBytes = append(rdbBytes, '$')
	rdbBytes = append(rdbBytes, []byte(strconv.Itoa(len(emptyRDB)))...)
	rdbBytes = append(rdbBytes, '\r', '\n')
	rdbBytes = append(rdbBytes, emptyRDB...)

	return resp.NewSimpleString(response), rdbBytes
}
