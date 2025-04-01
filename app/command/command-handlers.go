package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var store = NewKeyValueStore()

func setCommand(args []resp.RESP) resp.RESP {
	if len(args) < 1 {
		return resp.NewError("ERR wrong number of arguments for 'set' command")
	}

	key := args[0].String

	if len(args) < 2 {
		return resp.NewError("ERR wrong number of arguments for 'set' command")
	}

	value := args[1].String

	// store key value pair
	store.Set(key, value)

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

	// return the value as bulk string
	return resp.NewBulkString(value)
}
