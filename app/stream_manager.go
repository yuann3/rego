// app/stream_manager.go
package main

import (
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

func (sm *StreamManager) RegisterBlockedClient(key, startID string, timeout time.Duration) (chan []RESP, *time.Timer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	resultCh := make(chan []RESP, 1)
	timer := time.NewTimer(timeout)

	client := &BlockedClient{
		key:      key,
		startID:  startID,
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

	var remainingClients []*BlockedClient

	for _, client := range clients {
		startMs, startSeq, err := parseRangeID(client.startID, false)
		if err != nil {
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
			client.timeout.Stop()
			client.resultCh <- []RESP{
				NewBulkString(key),
				NewArray(streamEntries),
			}
			close(client.resultCh)
		} else {
			remainingClients = append(remainingClients, client)
		}
	}

	sm.blockedClients[key] = remainingClients
}

func (sm *StreamManager) RemoveBlockedClient(key string, resultCh chan []RESP) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	clients, exists := sm.blockedClients[key]
	if !exists {
		return
	}

	var remainingClients []*BlockedClient
	for _, client := range clients {
		if client.resultCh != resultCh {
			remainingClients = append(remainingClients, client)
		} else {
			client.timeout.Stop()
		}
	}

	if len(remainingClients) == 0 {
		delete(sm.blockedClients, key)
	} else {
		sm.blockedClients[key] = remainingClients
	}
}
