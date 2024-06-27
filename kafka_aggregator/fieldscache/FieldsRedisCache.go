package fieldscache

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type FieldsRedisCache struct {
	redisClient *redis.Client
}

func NewFieldsRedisCache(redisAddr string) *FieldsRedisCache {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &FieldsRedisCache{redisClient: redisClient}
}

func (frc *FieldsRedisCache) Get(id string, key string, ctx context.Context) (any, error) {
	return frc.redisClient.HGet(ctx, id, key).Result()
}

func (frc *FieldsRedisCache) Upsert(id string, key string, value any, ctx context.Context) error {
	return frc.redisClient.HSet(ctx, id, key, value).Err()
}
