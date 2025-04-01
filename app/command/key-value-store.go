package command

import (
	"sync"
)

type KeyValueStore struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
	}
}

// Set stores a key-value pair in the store
func (s *KeyValueStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a value by key, returns the value and a boolean indicating if the key exists
func (s *KeyValueStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}
