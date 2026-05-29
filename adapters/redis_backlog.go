package adapters

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var ErrGroupNotFound = errors.New("consumer group not found")

// Redis implements streamscaler.BacklogProvider
type RedisBacklog struct {
	client     *redis.Client
	streamName string
	groupName  string
}

func NewRedisBacklog(client *redis.Client, stream, group string) *RedisBacklog {
	return &RedisBacklog{
		client:     client,
		streamName: stream,
		groupName:  group,
	}
}

func (r *RedisBacklog) GetBacklog(ctx context.Context) (int64, error) {
	groups, err := r.client.XInfoGroups(ctx, r.streamName).Result()
	if err != nil {
		return 0, fmt.Errorf("could not get xinfogroups: %w", err)
	}

	for _, g := range groups {
		if g.Name == r.groupName {
			return g.Pending + g.Lag, nil
		}
	}
	return 0, fmt.Errorf("stream %q: %w", r.streamName, ErrGroupNotFound)
}
