package fieldscache

import (
	"context"
	"errors"
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

func (frc *FieldsRedisCache) Get(ctx context.Context, id string, key string) (string, error) {
	res, err := frc.redisClient.HGet(ctx, id, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return res, err
}

func (frc *FieldsRedisCache) Upsert(ctx context.Context, id string, key string, value any) error {
	return frc.redisClient.HSet(ctx, id, key, value).Err()
}
