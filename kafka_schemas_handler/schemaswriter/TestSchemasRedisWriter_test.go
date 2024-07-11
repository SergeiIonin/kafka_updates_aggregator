package schemaswriter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
	"kafka_updates_aggregator/domain"
	"kafka_updates_aggregator/redisprefixes"
	"kafka_updates_aggregator/test"
	"log"
	"slices"
	"testing"
	"time"
)

var (
	redisAddr          string
	redisContainer     tc.Container
	ctx                context.Context
	schemasRedisWriter *SchemasRedisWriter
	redisClient        *redis.Client
	redisPrefixes      redisprefixes.RedisPrefixes
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
	redisPrefixes = *redisprefixes.NewRedisPrefixes()
	schemasRedisWriter = NewSchemasRedisWriter(redisAddr, redisPrefixes.FieldPrefix, redisPrefixes.SchemaPrefix)
	redisClient = redis.NewClient(&redis.Options{Addr: redisAddr})
}

func TestSchemasRedisWriter_test(t *testing.T) {
	defer func(ctx context.Context, container tc.Container, t *testing.T) {
		err := test.TerminateTestContainer(ctx, container)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}(ctx, redisContainer, t)

	sc0 := domain.CreateSchema("user_balance_updates", 1, 1, []domain.Field{{"user_id", "string"}, {"balance", "int"}, {"deposit", "int"}, {"withdrawal", "int"}},
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

	sc1 := domain.CreateSchema("user_login", 1, 1, []domain.Field{{"user_id", "string"}, {"balance", "int"}, {"time", "string"}},
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
			redisClient.Del(ctx, fmt.Sprintf("%s%s", redisPrefixes.SchemaPrefix, schema.Key()))
			for _, field := range schema.Fields() {
				redisClient.HDel(ctx, fmt.Sprintf("%s%s", redisPrefixes.FieldPrefix, field))
			}
		}
	}

	t.Run("Add a schema", func(t *testing.T) {
		schema := sc0
		schemaKey, err := schemasRedisWriter.SaveSchema(ctx, *schema)
		assert.NoError(t, err)
		assert.Equal(t, schema.Key(), schemaKey)
		keys, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys: %v", keys)
		assert.Equal(t, 4, len(keys))

		schemaExists := redisClient.Exists(ctx, "schema.user_balance_updates.1").Val()
		assert.Equal(t, 1, int(schemaExists))

		schemaRawForUserId := redisClient.HGet(ctx, "field.user_id", schema.Key()).Val()
		var schemaDecoded domain.Schema
		err = json.Unmarshal([]byte(schemaRawForUserId), &schemaDecoded)
		assert.NoError(t, err)
		assert.Equal(t, schema.Key(), schemaDecoded.Key())
		assert.Equal(t, schema.ID(), schemaDecoded.ID())
		assert.Equal(t, schema.Schema(), schemaDecoded.Schema())
		assert.Equal(t, true, slices.Equal(schema.Fields(), schemaDecoded.Fields()))

		cleanupRedis([]domain.Schema{*sc0})
	})

	t.Run("Add schema twice", func(t *testing.T) {
		schema := sc0
		_, err := schemasRedisWriter.SaveSchema(ctx, *schema)
		assert.NoError(t, err)
		_, err = schemasRedisWriter.SaveSchema(ctx, *schema)
		msg := fmt.Sprintf("schema %s already exists in redis", sc0.Key())
		assert.Equal(t, err.Error(), msg)
		cleanupRedis([]domain.Schema{*sc0})
	})

	t.Run("Add two different schemas", func(t *testing.T) {
		schema0key, err := schemasRedisWriter.SaveSchema(ctx, *sc0)
		assert.NoError(t, err)
		assert.Equal(t, sc0.Key(), schema0key)
		keys0, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys: %v", keys0)
		assert.Equal(t, 4, len(keys0))

		schema0Exists := redisClient.Exists(ctx, "schema.user_balance_updates.1").Val()
		assert.Equal(t, 1, int(schema0Exists))

		schema1key, err := schemasRedisWriter.SaveSchema(ctx, *sc1)
		assert.NoError(t, err)
		assert.Equal(t, sc1.Key(), schema1key)
		keys1, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys: %v", keys1)
		assert.Equal(t, 5, len(keys1))

		schema1Exists := redisClient.Exists(ctx, "schema.user_login.1").Val()
		assert.Equal(t, 1, int(schema1Exists))

		schema0RawForUserId := redisClient.HGet(ctx, "field.user_id", sc0.Key()).Val()
		schema1RawForUserId := redisClient.HGet(ctx, "field.user_id", sc1.Key()).Val()
		var schema0DecodedForUserId domain.Schema
		var schema1DecodedForUserId domain.Schema

		err = json.Unmarshal([]byte(schema0RawForUserId), &schema0DecodedForUserId)
		assert.NoError(t, err)
		err = json.Unmarshal([]byte(schema1RawForUserId), &schema1DecodedForUserId)
		assert.NoError(t, err)

		assert.Equal(t, sc0.Key(), schema0DecodedForUserId.Key())
		assert.Equal(t, sc0.ID(), schema0DecodedForUserId.ID())
		assert.Equal(t, sc0.Schema(), schema0DecodedForUserId.Schema())
		assert.Equal(t, true, slices.Equal(sc0.Fields(), schema0DecodedForUserId.Fields()))

		assert.Equal(t, sc1.Key(), schema1DecodedForUserId.Key())
		assert.Equal(t, sc1.ID(), schema1DecodedForUserId.ID())
		assert.Equal(t, sc1.Schema(), schema1DecodedForUserId.Schema())
		assert.Equal(t, true, slices.Equal(sc1.Fields(), schema1DecodedForUserId.Fields()))

		cleanupRedis([]domain.Schema{*sc0, *sc1})
	})

	t.Run("Delete schema", func(t *testing.T) {
		_, err := schemasRedisWriter.SaveSchema(ctx, *sc0)
		assert.NoError(t, err)
		_, err = schemasRedisWriter.SaveSchema(ctx, *sc1)
		assert.NoError(t, err)

		keysBefore, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys before deletion: %v", keysBefore)
		assert.Equal(t, 5, len(keysBefore))

		schemaKey, err := schemasRedisWriter.DeleteSchema(ctx, sc0.Subject(), sc0.Version())
		assert.NoError(t, err)
		assert.Equal(t, sc0.Key(), schemaKey)

		keysAfter, err := redisClient.Keys(ctx, "field.*").Result()
		assert.NoError(t, err)
		t.Logf("redis keys after deletion: %v", keysAfter)
		assert.Equal(t, 3, len(keysAfter))

		schema0Deleted := redisClient.Exists(ctx, "schema.user_balance_updates.1").Val()
		assert.Equal(t, 0, int(schema0Deleted))

		schema1Exists := redisClient.Exists(ctx, "schema.user_login.1").Val()
		assert.Equal(t, 1, int(schema1Exists))

		schemasRawForUserId := redisClient.HGet(ctx, "field.user_id", sc1.Key()).Val()
		var schemaDecodedForUserId domain.Schema
		err = json.Unmarshal([]byte(schemasRawForUserId), &schemaDecodedForUserId)

		assert.NoError(t, err)
		assert.Equal(t, sc1.Key(), schemaDecodedForUserId.Key())
		assert.Equal(t, sc1.ID(), schemaDecodedForUserId.ID())
		assert.Equal(t, sc1.Schema(), schemaDecodedForUserId.Schema())
		assert.Equal(t, true, slices.Equal(sc1.Fields(), schemaDecodedForUserId.Fields()))

		cleanupRedis([]domain.Schema{*sc0, *sc1})
	})
}
