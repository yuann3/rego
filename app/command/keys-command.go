package command

import (
	"path/filepath"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

func keysCommand(args []resp.RESP) resp.RESP {
	if len(args) != 1 {
		return resp.NewError("ERR wrong number of arguments for 'keys' command")
	}

	pattern := args[0].String

	keys := store.GetAllKeys()

	var matchedKeys []resp.RESP
	for _, key := range keys {
		if matchPattern(pattern, key) {
			matchedKeys = append(matchedKeys, resp.NewBulkString(key))
		}
	}

	return resp.NewArray(matchedKeys)
}

func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}

	if !strings.Contains(pattern, "*") {
		return pattern == key
	}

	match, err := filepath.Match(pattern, key)
	if err != nil {
		return false
	}
	return match
}
