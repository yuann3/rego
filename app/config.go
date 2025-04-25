package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type ServerConfig struct {
	Dir         string
	DBFilename  string
	IsReplica   bool
	MasterHost  string
	MasterPort  int
	offset      int64
	offsetMutex sync.RWMutex
}

var defaultConfig = ServerConfig{
	Dir:        "./",
	DBFilename: "dump.rdb",
	IsReplica:  false,
	offset:     0,
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

func GetServerConfig() ServerConfig {
	return serverConfig
}

func IncrementOffset(bytesCount int64) {
	serverConfig.offsetMutex.Lock()
	defer serverConfig.offsetMutex.Unlock()

	oldOffset := serverConfig.offset
	serverConfig.offset += bytesCount

	fmt.Printf("Incremented offset from %d to %d (+%d bytes)\n",
		oldOffset, serverConfig.offset, bytesCount)

	IncrementMasterOffset(bytesCount)
}
