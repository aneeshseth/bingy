package cache

import (
	"fmt"

	"github.com/go-redis/redis"
)

type Cache struct {
	R *redis.Client
}

func NewCache(port string) *Cache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%s", port),
		Password: "",
		DB:       0,
	})
	c := &Cache{
		R: rdb,
	}
	return c
}
