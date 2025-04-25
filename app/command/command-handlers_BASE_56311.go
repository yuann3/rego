package command

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var masterReplID string
var masterReplOffset int = 0

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

func setCommand(args []resp.RESP) (resp.RESP, []byte) {
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

			// Parse expiry in seconds
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

func getCommand(args []resp.RESP) (resp.RESP, []byte) {
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

func keysCommand(args []resp.RESP) (resp.RESP, []byte) {
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

func infoCommand(args []resp.RESP) (resp.RESP, []byte) {
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
		info = fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", role, masterReplID, masterReplOffset)
	} else {
		info = fmt.Sprintf("role:%s", role)
	}

	return resp.NewBulkString(info), nil
}
