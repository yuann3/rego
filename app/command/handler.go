package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"strings"
)

type Handler func(args []resp.RESP) resp.RESP

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
}

func (r *Registry) Register(name string, handler Handler) {
	r.commands[strings.ToUpper(name)] = handler
}

func (r *Registry) Get(name string) (Handler, bool) {
	handler, ok := r.commands[strings.ToUpper(name)]
	return handler, ok
}

func pingCommand(args []resp.RESP) resp.RESP {
	if len(args) == 0 {
		return resp.NewSimpleString("PONG")
	}
	return resp.NewBulkString(args[0].String)
}

func echoCommand(args []resp.RESP) resp.RESP {
	if len(args) == 0 {
		return resp.NewError("ERR wrong number of arguments for 'echo' command")
	}
	return resp.NewBulkString(args[0].String)
}
