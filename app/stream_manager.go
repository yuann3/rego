package main

import (
    "sync"
    "time"
)

// BlockedClient tracks a blocked XREAD client.
type BlockedClient struct {
    key      string
    startID  string
    resultCh chan []RESP
    timeout  *time.Timer
}

// StreamManager coordinates blocking reads over streams.
type StreamManager struct {
    blockedClients map[string][]*BlockedClient
    mu             sync.RWMutex
}

var streamManager = &StreamManager{
    blockedClients: make(map[string][]*BlockedClient),
}

// GetStreamManager returns the singleton stream manager.
func GetStreamManager() *StreamManager {
    return streamManager
}

// RegisterBlockedClient registers a blocked client for XREAD on a key.
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

// NotifyNewEntry evaluates blocked clients and delivers new entries.
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
        startMs, startSeq, err := parseRangeID(client.startID, false, key)
        if err != nil {
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
    } else {
        sm.blockedClients[key] = remainingClients
    }
}

// RemoveBlockedClient unregisters a blocked client channel for a key.
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
        } else {
            sm.blockedClients[key] = remainingClients
        }
    }
}
