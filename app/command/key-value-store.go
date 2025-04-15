package command

import (
	"github.com/codecrafters-io/redis-starter-go/rdb"
	"sync"
	"time"
)

type KeyValueStore struct {
	data      map[string]string
	expiryMap map[string]time.Time
	mu        sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	store := &KeyValueStore{
		data:      make(map[string]string),
		expiryMap: make(map[string]time.Time),
	}

	go store.cleanupExpiredKeys()

	return store
}

func (s *KeyValueStore) Set(key, value string, expiry time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value

	// If expiry is set, store the expiration time
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
	return value, exists
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
			// Key has expired, schedule deletion
			go s.deleteExpiredKey(key)
			return false
		}
	}

	return true
}

func (s *KeyValueStore) GetAllKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	now := time.Now()

	for key := range s.data {
		// Skip keys that have expired
		if expiry, hasExpiry := s.expiryMap[key]; hasExpiry && now.After(expiry) {
			continue
		}
		keys = append(keys, key)
	}

	return keys
}

func (s *KeyValueStore) LoadFromRDB(dir, dbfilename string) error {
	keyData, expiryData, err := rdb.LoadRDBFile(dir, dbfilename)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear existing data
	s.data = make(map[string]string)
	s.expiryMap = make(map[string]time.Time)

	// Load new data
	for key, value := range keyData {
		s.data[key] = value
	}

	// Load expiry times
	for key, expiry := range expiryData {
		s.expiryMap[key] = expiry
	}

	return nil
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
