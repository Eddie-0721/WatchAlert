// Package testutil contains test instrumentation, not production Redis policy.
package testutil

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type redisProcessHook struct {
	wrap func(func(redis.Cmder) error) func(redis.Cmder) error
}

// WrapRedisProcess preserves command-counting/fault-injection tests during the
// v9 hook API migration without discarding the command's context.
func WrapRedisProcess(client *redis.Client, wrap func(func(redis.Cmder) error) func(redis.Cmder) error) {
	client.AddHook(redisProcessHook{wrap: wrap})
}

func (h redisProcessHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h redisProcessHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h redisProcessHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return h.wrap(func(command redis.Cmder) error { return next(ctx, command) })(cmd)
	}
}
