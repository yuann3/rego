package main

import (
    "fmt"
    "strconv"
    "strings"
    "sync"
)

// ServerConfig holds process-wide configuration and local offset.
type ServerConfig struct {
    Dir         string
    DBFilename  string
    IsReplica   bool
    MasterHost  string
    MasterPort  int
    offset      int64
    offsetMutex sync.RWMutex
}

var serverConfig = &ServerConfig{
    Dir:        "./",
    DBFilename: "dump.rdb",
    IsReplica:  false,
    offset:     0,
}

// InitConfig initializes the server configuration from CLI parameters.
func InitConfig(dir, dbfilename, replicaof string) error {
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

// GetServerConfig returns the global server configuration.
func GetServerConfig() *ServerConfig {
    return serverConfig
}

// IncrementOffset advances the local and master offsets by the given byte count.
func IncrementOffset(bytesCount int64) {
    serverConfig.offsetMutex.Lock()
    defer serverConfig.offsetMutex.Unlock()
    serverConfig.offset += bytesCount
    IncrementMasterOffset(bytesCount)
}
