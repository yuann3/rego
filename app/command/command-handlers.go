package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"strconv"
	"strings"
	"time"
)

var store = NewKeyValueStore()

func setCommand(args []resp.RESP) resp.RESP {
	if len(args) < 2 {
		return resp.NewError("ERR wrong number of arguments for 'set' command")
	}

	key := args[0].String
	value := args[1].String

	// Default: no expiry
	expiry := time.Duration(0)

	// Track NX/XX options
	var nx, xx bool

	for i := 2; i < len(args); i++ {
		option := strings.ToUpper(args[i].String)

		switch option {
		case "PX":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error")
			}

			ms, err := strconv.ParseInt(args[i+1].String, 10, 64)
			if err != nil || ms <= 0 {
				return resp.NewError("ERR value is not an integer or out of range")
			}

			expiry = time.Duration(ms) * time.Millisecond
			i++

		case "EX":
			if i+1 >= len(args) {
				return resp.NewError("ERR syntax error")
			}

			// Parse expiry in seconds
			seconds, err := strconv.ParseInt(args[i+1].String, 10, 64)
			if err != nil || seconds <= 0 {
				return resp.NewError("ERR value is not an integer or out of range")
			}

			expiry = time.Duration(seconds) * time.Second
			i++

		case "NX":
			nx = true
			if xx {
				return resp.NewError("ERR syntax error")
			}

		case "XX":
			xx = true
			if nx {
				return resp.NewError("ERR syntax error")
			}

		default:
			return resp.NewError("ERR syntax error")
		}
	}

	if nx {
		// Only set if the key does not exist
		if exists := store.Exists(key); exists {
			return resp.NewNullBulkString()
		}
	} else if xx {
		// Only set if the key already exists
		if exists := store.Exists(key); !exists {
			return resp.NewNullBulkString()
		}
	}

	// Store key value pair with expiry
	store.Set(key, value, expiry)

	return resp.NewSimpleString("OK")
}

func getCommand(args []resp.RESP) resp.RESP {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'get' command")
	}

	key := args[0].String

	value, exists := store.Get(key)

	if !exists {
		return resp.NewNullBulkString()
	}

	return resp.NewBulkString(value)
}
