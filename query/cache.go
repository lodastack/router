// this cache for improve http rquests performance
package query

import (
	"sync"
	"time"
)

type Cache struct {
	item map[string]interface{}
	mu   sync.RWMutex
}

func NewCache() *Cache {
	c := &Cache{
		item: make(map[string]interface{}),
	}
	return c
}

func (c *Cache) Set(key string, v interface{}) {
	c.mu.Lock()
	c.item[key] = v
	c.mu.Unlock()
}

func (c *Cache) Get(key string) interface{} {
	c.mu.RLock()
	v, ok := c.item[key]
	c.mu.RUnlock()
	if ok {
		return v
	}
	return nil
}

func (c *Cache) Purge() {
	c.mu.Lock()
	c.item = make(map[string]interface{})
	c.mu.Unlock()
}

func (c *Cache) purgeTimer() {
	// cache 6 hours
	ticker := time.NewTicker(time.Duration(360) * time.Minute)
	for {
		select {
		case <-ticker.C:
			c.Purge()
		}
	}
}
