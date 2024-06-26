package test_schemaswriter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
	"kafka_updates_aggregator/domain"
	"kafka_updates_aggregator/kafka_schemas_handler/schemaswriter"
	"log"
	"slices"
	"testing"
	"time"
)

var (
	redisAddr          string
	redisContainer     tc.Container
	ctx                context.Context
	schemasRedisWriter *schemaswriter.SchemasRedisWriter
	redisClient        *redis.Client
)

func init() {
	ctx = context.Background()

	req := tc.ContainerRequest{
		Image:        "redis:latest",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   tcWait.ForListeningPort("6379/tcp").WithStartupTimeout(60 * time.Second),
	}

	var err error
	redisContainer, err = tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf(err.Error())
	}

	host, err := redisContainer.Host(ctx)
	if err != nil {
		log.Fatalf(err.Error())
	}
	port, err := redisContainer.MappedPort(ctx, "6379/tcp")
	if err != nil {
		log.Fatalf(err.Error())
	}

	redisAddr = fmt.Sprintf("%s:%s", host, port.Port())
	schemasRedisWriter = schemaswriter.NewSchemasRedisWriter(redisAddr)
	redisClient = redis.NewClient(&redis.Options{Addr: redisAddr})
}

func TestSchemasRedisWriter_test(t *testing.T) {
	defer func(redisContainer tc.Container, ctx context.Context) {
		err := redisContainer.Terminate(ctx)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}(redisContainer, ctx)

	sc0 := domain.CreateSchema("user_balance_updates", 1, 1, []string{"user_id", "balance", "deposit", "withdrawal"},
		`{
						"type": "record",
						"name": "user_balance_updates",
						"fields": [
								{"name": "user_id", "type": "string"},
								{"name": "balance", "type": "int"},
								{"name": "deposit", "type": "int"},
								{"name": "withdrawal", "type": "int"}
							]
					}`)

	sc1 := domain.CreateSchema("user_login", 1, 1, []string{"user_id", "balance", "time"},
		`{
					"type": "record",
					"name": "user_login",
					"fields": [
							{"name": "user_id", "type": "string"},
							{"name": "balance", "type": "int"},
							{"name": "time", "type": "long"},
						]
				}`)

	cleanupRedis := func(schemas []domain.Schema) {
		for _, schema := range schemas {
			redisClient.Del(ctx, fmt.Sprintf("schema.%s", schema.Key()))
			for _, field := range schema.Fields() {
				redisClient.HDel(ctx, fmt.Sprintf("field.%s", field), "schemas")
			}
		}
	}

	t.Run("Add a schema", func(t *testing.T) {
		schema := sc0
		schemaKey, err := schemasRedisWriter.SaveSchema(*schema, ctx)
		assert.NoError(t, err)
		assert.Equal(t, schema.Key(), schemaKey)
		keys, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys: %v", keys)
		assert.Equal(t, 4, len(keys))

		schemaExists := redisClient.Get(ctx, "schema.user_balance_updates.1").Val()
		assert.Equal(t, "1", schemaExists)

		schemasRawForUserId := redisClient.HGet(ctx, "field.user_id", "schemas").Val()
		var schemasForUserId []domain.Schema
		err = json.Unmarshal([]byte(schemasRawForUserId), &schemasForUserId)
		schemaDecoded := schemasForUserId[0]
		assert.NoError(t, err)
		assert.Equal(t, 1, len(schemasForUserId))
		assert.Equal(t, schema.Key(), schemaDecoded.Key())
		assert.Equal(t, schema.ID(), schemaDecoded.ID())
		assert.Equal(t, schema.Schema(), schemaDecoded.Schema())
		assert.Equal(t, true, slices.Equal(schema.Fields(), schemaDecoded.Fields()))

		cleanupRedis([]domain.Schema{*sc0})
	})

	t.Run("Add schema twice", func(t *testing.T) {
		schema := sc0
		_, err := schemasRedisWriter.SaveSchema(*schema, ctx)
		assert.NoError(t, err)
		_, err = schemasRedisWriter.SaveSchema(*schema, ctx)
		msg := fmt.Sprintf("schema %s already exists in redis", sc0.Key())
		assert.Equal(t, err.Error(), msg)
		cleanupRedis([]domain.Schema{*sc0})
	})

	t.Run("Add two different schemas", func(t *testing.T) {
		schema0key, err := schemasRedisWriter.SaveSchema(*sc0, ctx)
		assert.NoError(t, err)
		assert.Equal(t, sc0.Key(), schema0key)
		keys0, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys: %v", keys0)
		assert.Equal(t, 4, len(keys0))

		schema0Exists := redisClient.Get(ctx, "schema.user_balance_updates.1").Val()
		assert.Equal(t, "1", schema0Exists)

		schema1key, err := schemasRedisWriter.SaveSchema(*sc1, ctx)
		assert.NoError(t, err)
		assert.Equal(t, sc1.Key(), schema1key)
		keys1, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys: %v", keys1)
		assert.Equal(t, 5, len(keys1))

		schema1Exists := redisClient.Get(ctx, "schema.user_login.1").Val()
		assert.Equal(t, "1", schema1Exists)

		schemasRawForUserId := redisClient.HGet(ctx, "field.user_id", "schemas").Val()
		var schemasForUserId []domain.Schema
		err = json.Unmarshal([]byte(schemasRawForUserId), &schemasForUserId)
		schema0Decoded := schemasForUserId[0]
		assert.NoError(t, err)
		assert.Equal(t, 2, len(schemasForUserId))
		assert.Equal(t, sc0.Key(), schema0Decoded.Key())
		assert.Equal(t, sc0.ID(), schema0Decoded.ID())
		assert.Equal(t, sc0.Schema(), schema0Decoded.Schema())
		assert.Equal(t, true, slices.Equal(sc0.Fields(), schema0Decoded.Fields()))

		schema1Decoded := schemasForUserId[1]
		assert.NoError(t, err)
		assert.Equal(t, sc1.Key(), schema1Decoded.Key())
		assert.Equal(t, sc1.ID(), schema1Decoded.ID())
		assert.Equal(t, sc1.Schema(), schema1Decoded.Schema())
		assert.Equal(t, true, slices.Equal(sc1.Fields(), schema1Decoded.Fields()))

		cleanupRedis([]domain.Schema{*sc0, *sc1})
	})
}