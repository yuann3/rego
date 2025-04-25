package command

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

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
	commands        map[string]Handler
	isWriteCmd      map[string]bool
	clientOffsets   map[string]int
	replicaAcks     map[net.Conn]int
	clientOffsetsMu sync.RWMutex
	replicaAcksMu   sync.RWMutex
	waitCond        *sync.Cond
}

func NewRegistry() *Registry {
	r := &Registry{
		commands:      make(map[string]Handler),
		isWriteCmd:    make(map[string]bool),
		clientOffsets: make(map[string]int),
		replicaAcks:   make(map[net.Conn]int),
	}

	var mu sync.Mutex
	r.waitCond = sync.NewCond(&mu)

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

func (r *Registry) SetClientOffset(clientID string, offset int) {
	r.clientOffsetsMu.Lock()
	defer r.clientOffsetsMu.Unlock()
	r.clientOffsets[clientID] = offset
}

func (r *Registry) GetClientOffset(clientID string) int {
	r.clientOffsetsMu.RLock()
	defer r.clientOffsetsMu.RUnlock()
	offset, exists := r.clientOffsets[clientID]
	if !exists {
		return 0
	}
	return offset
}

func (r *Registry) UpdateReplicaAck(conn net.Conn, offset int) {
	r.replicaAcksMu.Lock()
	r.replicaAcks[conn] = offset
	r.replicaAcksMu.Unlock()
	r.waitCond.Broadcast()
}

func (r *Registry) CountAcknowledgingReplicas(clientOffset int) int {
	r.replicaAcksMu.RLock()
	defer r.replicaAcksMu.RUnlock()

	count := 0
	for _, offset := range r.replicaAcks {
		if offset >= clientOffset {
			count++
		}
	}
	return count
}

func (r *Registry) GetTotalReplicas() int {
	r.replicaAcksMu.RLock()
	defer r.replicaAcksMu.RUnlock()
	return len(r.replicaAcks)
}

func (r *Registry) RemoveReplica(conn net.Conn) {
	r.replicaAcksMu.Lock()
	delete(r.replicaAcks, conn)
	r.replicaAcksMu.Unlock()
	r.waitCond.Broadcast()
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
