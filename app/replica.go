package main

import (
	"fmt"
	"math/rand"
	"net"
	"slices"
	"sync"
	"time"
)

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
var masterReplOffset int = 0

func init() {
	masterReplID = generateReplID()
	fmt.Printf("Initialized master with replication ID: %s\n", masterReplID)
}

func generateReplID() string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 40)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func AddReplica(conn net.Conn) {
	replicaMu.Lock()
	defer replicaMu.Unlock()

	// Check if this replica is already registered
	for _, r := range replicas {
		if r.Conn == conn {
			fmt.Printf("Replica already registered: %s\n", conn.RemoteAddr())
			return
		}
	}

	// Add the new replica
	replicas = append(replicas, &ReplicaState{
		Conn:        conn,
		Offset:      0,
		LastAckTime: time.Now(),
	})
	fmt.Printf("Added new replica: %s, total replicas: %d\n", conn.RemoteAddr(), len(replicas))
}

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

func GetReplicaCount() int {
	replicaMu.RLock()
	defer replicaMu.RUnlock()
	return len(replicas)
}

func GetReplicaConnections() []net.Conn {
	replicaMu.RLock()
	defer replicaMu.RUnlock()
	conns := make([]net.Conn, len(replicas))
	for i, r := range replicas {
		conns[i] = r.Conn
	}
	return conns
}

func IncrementMasterOffset(bytesCount int64) {
	offsetMu.Lock()
	defer offsetMu.Unlock()
	oldOffset := currentOffset
	currentOffset += bytesCount
	masterReplOffset += int(bytesCount)
	fmt.Printf("Incremented offset from %d to %d (+%d bytes)\n",
		oldOffset, currentOffset, bytesCount)
}

func GetOffset() int64 {
	offsetMu.RLock()
	defer offsetMu.RUnlock()
	return currentOffset
}

func GetMasterOffset() int64 {
	offsetMu.RLock()
	defer offsetMu.RUnlock()
	return currentOffset
}

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
