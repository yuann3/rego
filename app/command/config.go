package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type ServerConfig struct {
	Dir        string
	DBFilename string
	IsReplica  bool
	MasterHost string
	MasterPort int
}

var defaultConfig = ServerConfig{
	Dir:        "./",
	DBFilename: "dump.rdb",
	IsReplica:  false,
}

var serverConfig = defaultConfig

func InitConfig(dir, dbfilename string, replicaof string) error {
	if dir != "" {
		serverConfig.Dir = dir
	}
	if dbfilename != "" {
		serverConfig.DBFilename = dbfilename
	}
	if replicaof != "" {
		parts := strings.Fields(replicaof)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --replicaof format: expected 'host port', got '%s'", replicaof)
		}
		serverConfig.MasterHost = parts[0]
		port, err := strconv.Atoi(parts[1])
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("invalid master port: %s", parts[1])
		}
		serverConfig.MasterPort = port
		serverConfig.IsReplica = true
	}
	return nil
}

func configCommand(args []resp.RESP) resp.RESP {
	if len(args) < 1 {
		return resp.NewError("ERR wrong number of arguments for 'config' command")
	}

	subCommand := strings.ToUpper(args[0].String)

	switch subCommand {
	case "GET":
		return configGetCommand(args[1:])
	default:
		return resp.NewError("ERR unknown subcommand '" + subCommand + "'. Try CONFIG GET")
	}
}

func configGetCommand(args []resp.RESP) resp.RESP {
	if len(args) < 1 {
		return resp.NewError("ERR wrong number of arguments for 'config get' command")
	}

	pattern := strings.ToLower(args[0].String)
	resultPairs := make([]resp.RESP, 0)

	switch pattern {
	case "dir":
		resultPairs = append(resultPairs, resp.NewBulkString("dir"))
		resultPairs = append(resultPairs, resp.NewBulkString(serverConfig.Dir))
	case "dbfilename":
		resultPairs = append(resultPairs, resp.NewBulkString("dbfilename"))
		resultPairs = append(resultPairs, resp.NewBulkString(serverConfig.DBFilename))
	case "*":
		resultPairs = append(resultPairs, resp.NewBulkString("dir"))
		resultPairs = append(resultPairs, resp.NewBulkString(serverConfig.Dir))
		resultPairs = append(resultPairs, resp.NewBulkString("dbfilename"))
		resultPairs = append(resultPairs, resp.NewBulkString(serverConfig.DBFilename))
	default:
		return resp.NewArray(resultPairs)
	}

	return resp.NewArray(resultPairs)
}

func GetServerConfig() ServerConfig {
	return serverConfig
}
