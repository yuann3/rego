package command

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var masterReplID string
var masterReplOffset int = 0
var masterReplOffsetMu sync.RWMutex
var registryInstance *Registry

func init() {
	masterReplID = generateReplID()
}

func generateReplID() string {
	b := make([]byte, 40)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

<<<<<<< HEAD
func InitRegistry(registry *Registry) {
	registryInstance = registry
}

func GetRegistry() *Registry {
	return registryInstance
}

func GetMasterReplOffset() int {
	masterReplOffsetMu.RLock()
	defer masterReplOffsetMu.RUnlock()
	return masterReplOffset
}

func IncrementMasterReplOffset(size int) {
	masterReplOffsetMu.Lock()
	defer masterReplOffsetMu.Unlock()
	masterReplOffset += size
}

func setCommand(args []resp.RESP) (resp.RESP, []byte) {
=======
func GetMasterOffset() int {
	return masterReplOffset
}

func IncrementMasterOffset(increment int) {
	masterReplOffset += increment
}

func setCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
>>>>>>> b1c921c6533d758bdade44511055546da8894670
	if len(args) < 2 {
		return resp.NewError("ERR wrong number of arguments for 'set' command"), nil
	}

	key := args[0].String
	value := args[1].String

	expiry := time.Duration(0)

	var nx, xx bool

	for i := 2; i < len(args); i++ {
		option := strings.ToUpper(args[i].String)

		switch option {
		case "PX":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error"), nil
			}

			ms, err := strconv.ParseInt(args[i+1].String, 10, 64)
			if err != nil || ms <= 0 {
				return resp.NewError("ERR value is not an integer or out of range"), nil
			}

			expiry = time.Duration(ms) * time.Millisecond
			i++

		case "EX":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error"), nil
			}

			seconds, err := strconv.ParseInt(args[i+1].String, 10, 64)
			if err != nil || seconds <= 0 {
				return resp.NewError("ERR value is not an integer or out of range"), nil
			}

			expiry = time.Duration(seconds) * time.Second
			i++

		case "NX":
			nx = true
			if xx {
				return resp.NewError("ERR syntax error"), nil
			}

		case "XX":
			xx = true
			if nx {
				return resp.NewError("ERR syntax error"), nil
			}

		default:
			return resp.NewError("ERR syntax error"), nil
		}
	}

	if nx {
		if exists := GetStore().Exists(key); exists {
			return resp.NewNullBulkString(), nil
		}
	} else if xx {
		if exists := GetStore().Exists(key); !exists {
			return resp.NewNullBulkString(), nil
		}
	}

	GetStore().Set(key, value, expiry)

	return resp.NewSimpleString("OK"), nil
}

func getCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'get' command"), nil
	}

	key := args[0].String

	value, exists := GetStore().Get(key)

	if !exists {
		return resp.NewNullBulkString(), nil
	}

	return resp.NewBulkString(value), nil
}

func keysCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'keys' command"), nil
	}

	pattern := args[0].String

	allKeys := GetStore().Keys()

	var matchedKeys []string

	if pattern == "*" {
		matchedKeys = allKeys
	} else if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		for _, key := range allKeys {
			if strings.HasPrefix(key, prefix) {
				matchedKeys = append(matchedKeys, key)
			}
		}
	} else {
		for _, key := range allKeys {
			if key == pattern {
				matchedKeys = append(matchedKeys, key)
			}
		}
	}

	items := make([]resp.RESP, len(matchedKeys))
	for i, key := range matchedKeys {
		items[i] = resp.NewBulkString(key)
	}

	return resp.NewArray(items), nil
}

func infoCommand(args []resp.RESP, conn net.Conn) (resp.RESP, []byte) {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'info' command"), nil
	}

	if strings.ToUpper(args[0].String) != "REPLICATION" {
		return resp.NewError("ERR only replication section is supported"), nil
	}

	role := "master"
	if GetServerConfig().IsReplica {
		role = "slave"
	}

	var info string
	if role == "master" {
		info = fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", role, masterReplID, GetMasterReplOffset())
	} else {
		info = fmt.Sprintf("role:%s", role)
	}

	return resp.NewBulkString(info), nil
}

func waitCommand(args []resp.RESP) (resp.RESP, []byte) {
	if len(args) != 2 {
		return resp.NewError("ERR wrong number of arguments for 'wait' command"), nil
	}

	numReplicas, err := strconv.Atoi(args[0].String)
	if err != nil || numReplicas < 0 {
		return resp.NewError("ERR numreplicas must be a non-negative integer"), nil
	}

	timeout, err := strconv.Atoi(args[1].String)
	if err != nil || timeout < 0 {
		return resp.NewError("ERR timeout must be a non-negative integer"), nil
	}

	registry := GetRegistry()
	clientID := "client" // Using a fixed clientID for testing

	// Get the maximum offset from the client's write commands
	clientOffset := registry.GetMaxClientOffset(clientID)

	// Edge case: No previous write commands or no replicas
	if clientOffset == 0 || registry.GetTotalReplicas() == 0 {
		totalReplicas := registry.GetTotalReplicas()
		return resp.NewInteger(totalReplicas), nil
	}

	// Check if enough replicas have already acknowledged
	ackCount := registry.CountAcknowledgingReplicas(clientOffset)
	totalReplicas := registry.GetTotalReplicas()

	if numReplicas == 0 || ackCount >= numReplicas || totalReplicas == 0 {
		return resp.NewInteger(ackCount), nil
	}

	// Zero timeout means block indefinitely
	if timeout == 0 {
		registry.waitCond.L.Lock()
		defer registry.waitCond.L.Unlock()

		for {
			ackCount = registry.CountAcknowledgingReplicas(clientOffset)
			if ackCount >= numReplicas || ackCount >= totalReplicas {
				break
			}
			registry.waitCond.Wait()
		}

		return resp.NewInteger(ackCount), nil
	}

	// Non-zero timeout
	timeoutCh := time.After(time.Duration(timeout) * time.Millisecond)
	doneCh := make(chan struct{})

	go func() {
		registry.waitCond.L.Lock()
		defer registry.waitCond.L.Unlock()

		for {
			ackCount := registry.CountAcknowledgingReplicas(clientOffset)
			if ackCount >= numReplicas || ackCount >= totalReplicas {
				close(doneCh)
				return
			}

			// Create a waiter channel for the condition
			waiterCh := make(chan struct{})
			go func() {
				registry.waitCond.Wait()
				close(waiterCh)
			}()

			registry.waitCond.L.Unlock()

			select {
			case <-waiterCh:
				// Condition was signaled
				registry.waitCond.L.Lock()
				continue
			case <-timeoutCh:
				// Timeout occurred
				registry.waitCond.L.Lock()
				close(doneCh)
				return
			}
		}
	}()

	// Wait for either completion or timeout
	<-doneCh

	// Get final acknowledgment count
	ackCount = registry.CountAcknowledgingReplicas(clientOffset)

	// Clear client offsets after WAIT completes
	registry.ClearClientOffsets(clientID)

	return resp.NewInteger(ackCount), nil
}
