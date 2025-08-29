package main

import (
    "math/rand"
    "net"
    "slices"
    "sync"
    "time"
)

// ReplicaState tracks replication progress for a connected replica.
type ReplicaState struct {
    Conn        net.Conn
    Offset      int64
    LastAckTime time.Time
}

var (
    replicas      []*ReplicaState
    replicaMu     sync.RWMutex
    currentOffset int64
    offsetMu      sync.RWMutex
)

var masterReplID string
var masterReplOffset int64 = 0

func init() {
    masterReplID = generateReplID()
    _ = masterReplID
}

// generateReplID returns a random 40-character replication ID.
func generateReplID() string {
    charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    b := make([]byte, 40)
    for i := range b {
        b[i] = charset[rand.Intn(len(charset))]
    }
    return string(b)
}

// AddReplica registers a new replica connection.
func AddReplica(conn net.Conn) {
    replicaMu.Lock()
    defer replicaMu.Unlock()

    for _, r := range replicas {
        if r.Conn == conn {
            return
        }
    }

    replicas = append(replicas, &ReplicaState{
        Conn:        conn,
        Offset:      0,
        LastAckTime: time.Now(),
    })
}

// RemoveReplica removes a replica connection.
func RemoveReplica(conn net.Conn) {
    replicaMu.Lock()
    defer replicaMu.Unlock()
    for i, r := range replicas {
        if r.Conn == conn {
            replicas = slices.Delete(replicas, i, i+1)
            break
        }
    }
}

// UpdateReplicaOffset records the latest acknowledged offset for a replica.
func UpdateReplicaOffset(conn net.Conn, offset int64) {
    replicaMu.Lock()
    defer replicaMu.Unlock()
    for _, r := range replicas {
        if r.Conn == conn {
            r.Offset = offset
            r.LastAckTime = time.Now()
            break
        }
    }
}

// GetReplicaCount returns the number of connected replicas.
func GetReplicaCount() int {
    replicaMu.RLock()
    defer replicaMu.RUnlock()
    return len(replicas)
}

// GetReplicaConnections returns a snapshot of active replica connections.
func GetReplicaConnections() []net.Conn {
    replicaMu.RLock()
    defer replicaMu.RUnlock()
    conns := make([]net.Conn, len(replicas))
    for i, r := range replicas {
        conns[i] = r.Conn
    }
    return conns
}

// IncrementMasterOffset advances the master replication offset.
func IncrementMasterOffset(bytesCount int64) {
    offsetMu.Lock()
    defer offsetMu.Unlock()
    currentOffset += bytesCount
    masterReplOffset += bytesCount
}

// GetOffset returns the current local offset.
func GetOffset() int64 {
    offsetMu.RLock()
    defer offsetMu.RUnlock()
    return currentOffset
}

// GetMasterOffset returns the current master offset.
func GetMasterOffset() int64 {
    offsetMu.RLock()
    defer offsetMu.RUnlock()
    return currentOffset
}

// WaitForReplicas waits until the specified number of replicas ack the target offset or timeout.
func WaitForReplicas(count int, timeoutMs int) int {
    if count <= 0 || GetReplicaCount() == 0 {
        return 0
    }

    targetOffset := GetMasterOffset()
    timeout := time.Duration(timeoutMs) * time.Millisecond
    endTime := time.Now().Add(timeout)

    for time.Now().Before(endTime) {
        ackCount := GetAcknowledgedReplicaCount(targetOffset)
        if ackCount >= count {
            return ackCount
        }
        time.Sleep(10 * time.Millisecond)
    }

    return GetAcknowledgedReplicaCount(targetOffset)
}

// GetAcknowledgedReplicaCount returns the number of replicas that have reached the given offset.
func GetAcknowledgedReplicaCount(targetOffset int64) int {
    replicaMu.RLock()
    defer replicaMu.RUnlock()

    count := 0
    for _, r := range replicas {
        if r.Offset >= targetOffset {
            count++
        }
    }
    return count
}
