package client

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"
	"watchAlert/config"

	"github.com/redis/go-redis/v9"
)

var Redis *redis.Client

func InitRedis() *redis.Client {

	client := redis.NewClient(RedisOptions())

	// Startup is bounded independently of later request contexts.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		_ = client.Close()
		log.Printf("redis Connection Failed %s", err)
		panic(err)
	}
	Redis = client
	return client
}

// RedisOptions makes compatibility-sensitive defaults explicit. In particular,
// v9's zero MaxRetries means retries, whereas our v6 client did not retry.
func RedisOptions() *redis.Options {
	return &redis.Options{
		Addr:                  fmt.Sprintf("%s:%s", config.Application.Redis.Host, config.Application.Redis.Port),
		Password:              config.Application.Redis.Pass,
		DB:                    config.Application.Redis.Database, // 使用默认的数据库
		Protocol:              2,
		DisableIdentity:       true,
		ContextTimeoutEnabled: true,
		MaxRetries:            -1,
		DialerRetries:         -1,
		DialTimeout:           5 * time.Second,
		ReadTimeout:           3 * time.Second,
		WriteTimeout:          3 * time.Second,
		PoolTimeout:           4 * time.Second,
		PoolSize:              10 * runtime.NumCPU(),
		ConnMaxIdleTime:       5 * time.Minute,
		// v6 used bufio's 4 KiB buffers; avoid an unrelated 8x pool-buffer increase.
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	}
}
