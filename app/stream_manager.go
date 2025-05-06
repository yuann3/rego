package main

import (
	"fmt"
	"sync"
	"time"
)

type BlockedClient struct {
	key      string
	startID  string
	resultCh chan []RESP
	timeout  *time.Timer
}

type StreamManager struct {
	blockedClients map[string][]*BlockedClient
	mu             sync.RWMutex
}

var streamManager = &StreamManager{
	blockedClients: make(map[string][]*BlockedClient),
}

func GetStreamManager() *StreamManager {
	return streamManager
}

func (sm *StreamManager) RegisterBlockedClient(key, requestedID string, timeout time.Duration) (chan []RESP, *time.Timer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	resolvedStartID := requestedID

	if requestedID == "$" {
		stream, exists := GetStore().GetStream(key)
		if exists && len(stream.Entries) > 0 {
			resolvedStartID = stream.Entries[len(stream.Entries)-1].ID
		} else {
			resolvedStartID = "0-0"
		}
		fmt.Printf("Resolved '$' for key '%s' to start ID '%s'\n", key, resolvedStartID)
	}

	resultCh := make(chan []RESP, 1)
	var timer *time.Timer

	if timeout > 0 {
		timer = time.NewTimer(timeout)
	}

	client := &BlockedClient{
		key:      key,
		startID:  resolvedStartID,
		resultCh: resultCh,
		timeout:  timer,
	}

	sm.blockedClients[key] = append(sm.blockedClients[key], client)
	return resultCh, timer
}

func (sm *StreamManager) NotifyNewEntry(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	clients, exists := sm.blockedClients[key]
	if !exists || len(clients) == 0 {
		return
	}

	store := GetStore()
	stream, exists := store.GetStream(key)
	if !exists || len(stream.Entries) == 0 {
		return
	}

	fmt.Printf("NotifyNewEntry called for key '%s'. Checking %d blocked clients.\n", key, len(clients))

	var remainingClients []*BlockedClient

	for _, client := range clients {
		startMs, startSeq, err := parseRangeID(client.startID, false, key)
		if err != nil {
			fmt.Printf("Error parsing stored startID '%s' for client on key '%s': %v\n", client.startID, key, err)
			remainingClients = append(remainingClients, client)
			continue
		}

		var streamEntries []RESP
		for _, entry := range stream.Entries {
			entryMs, entrySeq, err := splitStreamID(entry.ID)
			if err != nil {
				continue
			}

			if compareStreamIDs(startMs, startSeq, entryMs, entrySeq) < 0 {

				fieldValues := make([]RESP, 0, len(entry.Fields)*2)
				for field, value := range entry.Fields {
					fieldValues = append(fieldValues, NewBulkString(field))
					fieldValues = append(fieldValues, NewBulkString(value))
				}

				entryArray := NewArray([]RESP{
					NewBulkString(entry.ID),
					NewArray(fieldValues),
				})

				streamEntries = append(streamEntries, entryArray)
			}
		}

		if len(streamEntries) > 0 {
			fmt.Printf("Found %d new entries for client on key '%s' starting after '%s'. Notifying.\n", len(streamEntries), key, client.startID)
			if client.timeout != nil {
				client.timeout.Stop()
			}
			client.resultCh <- []RESP{
				NewBulkString(key),
				NewArray(streamEntries),
			}
			close(client.resultCh)
		} else {
			remainingClients = append(remainingClients, client)
		}
	}

	if len(remainingClients) == 0 {
		delete(sm.blockedClients, key)
		fmt.Printf("No remaining blocked clients for key '%s'.\n", key)
	} else {
		sm.blockedClients[key] = remainingClients
		fmt.Printf("%d blocked clients remain for key '%s'.\n", len(remainingClients), key)
	}
}

func (sm *StreamManager) RemoveBlockedClient(key string, resultCh chan []RESP) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	clients, exists := sm.blockedClients[key]
	if !exists {
		return
	}

	var remainingClients []*BlockedClient
	found := false
	for _, client := range clients {
		if client.resultCh != resultCh {
			remainingClients = append(remainingClients, client)
		} else {
			found = true
			if client.timeout != nil {
				client.timeout.Stop()
			}
		}
	}

	if found {
		if len(remainingClients) == 0 {
			delete(sm.blockedClients, key)
			fmt.Printf("Removed last blocked client for key '%s'.\n", key)
		} else {
			sm.blockedClients[key] = remainingClients
			fmt.Printf("Removed blocked client. %d remain for key '%s'.\n", len(remainingClients), key)
		}
	}
}
