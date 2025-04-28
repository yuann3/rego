package main

import (
	"sync"
	"time"
)

type KeyValueStore struct {
	data      map[string]interface{}
	expiryMap map[string]time.Time
	mu        sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	store := &KeyValueStore{
		data:      make(map[string]interface{}),
		expiryMap: make(map[string]time.Time),
	}

	go store.cleanupExpiredKeys()

	return store
}

func (s *KeyValueStore) Set(key string, value interface{}, expiry time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value

	if expiry > 0 {
		s.expiryMap[key] = time.Now().Add(expiry)
	} else if _, exists := s.expiryMap[key]; exists {
		delete(s.expiryMap, key)
	}
}

func (s *KeyValueStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if expiry, hasExpiry := s.expiryMap[key]; hasExpiry {
		if time.Now().After(expiry) {
			go s.deleteExpiredKey(key)
			return "", false
		}
	}

	value, exists := s.data[key]
	if !exists {
		return "", false
	}

	str, ok := value.(string)
	if !ok {
		return "", false
	}

	return str, true
}

func (s *KeyValueStore) GetStream(key string) (*Stream, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if expiry, hasExpiry := s.expiryMap[key]; hasExpiry {
		if time.Now().After(expiry) {
			go s.deleteExpiredKey(key)
			return nil, false
		}
	}

	value, exists := s.data[key]
	if !exists {
		return nil, false
	}

	stream, ok := value.(*Stream)
	if !ok {
		return nil, false
	}

	return stream, true
}

func (s *KeyValueStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	now := time.Now()

	for key := range s.data {
		if expiry, hasExpiry := s.expiryMap[key]; hasExpiry && now.After(expiry) {
			continue
		}
		keys = append(keys, key)
	}

	return keys
}

func (s *KeyValueStore) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.data[key]
	if !exists {
		return false
	}

	if expiry, hasExpiry := s.expiryMap[key]; hasExpiry {
		if time.Now().After(expiry) {
			go s.deleteExpiredKey(key)
			return false
		}
	}

	return true
}

func (s *KeyValueStore) GetType(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.Exists(key) {
		return "none"
	}

	value, exists := s.data[key]
	if !exists {
		return "none"
	}

	switch value.(type) {
	case string:
		return "string"
	case *Stream:
		return "stream"
	default:
		return "none"
	}
}

func (s *KeyValueStore) deleteExpiredKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if expiry, hasExpiry := s.expiryMap[key]; hasExpiry {
		if time.Now().After(expiry) {
			delete(s.data, key)
			delete(s.expiryMap, key)
		}
	}
}

func (s *KeyValueStore) cleanupExpiredKeys() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()

		expiredKeys := make([]string, 0)
		for key, expiry := range s.expiryMap {
			if now.After(expiry) {
				expiredKeys = append(expiredKeys, key)
			}
		}

		for _, key := range expiredKeys {
			delete(s.data, key)
			delete(s.expiryMap, key)
		}

		s.mu.Unlock()
	}
}

var storeInstance *KeyValueStore

func init() {
	storeInstance = NewKeyValueStore()
}

func GetStore() *KeyValueStore {
	return storeInstance
}
