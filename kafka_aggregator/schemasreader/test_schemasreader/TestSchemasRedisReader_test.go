package test_schemasreader

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
	"kafka_updates_aggregator/domain"
	"kafka_updates_aggregator/infra"
	"kafka_updates_aggregator/kafka_aggregator/schemasreader"
	"kafka_updates_aggregator/testutils"
	"log"
	"slices"
	"testing"
	"time"
)

var (
	redisAddr          string
	redisContainer     tc.Container
	ctx                context.Context
	schemasRedisReader *schemasreader.SchemasRedisReader
	redisClient        *redis.Client
	redisPrefixes      infra.RedisPrefixes
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
	redisPrefixes = *infra.NewRedisPrefixes()
	schemasRedisReader = schemasreader.NewSchemasRedisReader(redisAddr, redisPrefixes.FieldPrefix, redisPrefixes.SchemaPrefix)
	redisClient = redis.NewClient(&redis.Options{Addr: redisAddr})
}

func TestSchemasRedisReader_test(t *testing.T) {
	defer func(container tc.Container, ctx context.Context, t *testing.T) {
		err := testutils.TerminateTestContainer(container, ctx, t)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}(redisContainer, ctx, t)

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
			redisClient.Del(ctx, fmt.Sprintf("%s%s", redisPrefixes.SchemaPrefix, schema.Key()))
			for _, field := range schema.Fields() {
				redisClient.HDel(ctx, fmt.Sprintf("%s%s", redisPrefixes.FieldPrefix, field))
			}
		}
	}

	t.Run("Read nothing if there's no schemas for field", func(t *testing.T) {
		field := "user_id"
		res, err := schemasRedisReader.GetSchemasForField(field, ctx)

		assert.NoError(t, err)
		assert.Equal(t, 0, len(res))

		cleanupRedis(nil)
	})

	t.Run("Fetch one schema", func(t *testing.T) {
		field := "user_id"
		fieldRedisKey := fmt.Sprintf("%s%s", redisPrefixes.FieldPrefix, field)
		schemaRaw, _ := json.Marshal(sc0)

		err := redisClient.HSet(ctx, fieldRedisKey, sc0.Key(), string(schemaRaw)).Err()
		assert.NoError(t, err)
		schemasFetched, err := schemasRedisReader.GetSchemasForField(field, ctx)
		assert.NoError(t, err)

		t.Logf("schema's key = %s", schemasFetched[0].Key())
		assert.Equal(t, 1, len(schemasFetched))
		assert.Equal(t, sc0.Key(), schemasFetched[0].Key())
		assert.Equal(t, sc0.ID(), schemasFetched[0].ID())
		assert.Equal(t, sc0.Schema(), schemasFetched[0].Schema())
		assert.Equal(t, true, slices.Equal(sc0.Fields(), schemasFetched[0].Fields()))

		cleanupRedis(schemasFetched)
	})

	t.Run("Fetch multiple schemas", func(t *testing.T) {
		field := "user_id"
		fieldRedisKey := fmt.Sprintf("%s%s", redisPrefixes.FieldPrefix, field)

		schema0Raw, _ := json.Marshal(sc0)
		err := redisClient.HSet(ctx, fieldRedisKey, sc0.Key(), string(schema0Raw)).Err()
		assert.NoError(t, err)

		schema1Raw, _ := json.Marshal(sc1)
		err = redisClient.HSet(ctx, fieldRedisKey, sc1.Key(), string(schema1Raw)).Err()
		assert.NoError(t, err)

		schemasFetched, err := schemasRedisReader.GetSchemasForField(field, ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(schemasFetched))

		t.Logf("schema[0] key = %s", schemasFetched[0].Key())
		t.Logf("schema[1] key = %s", schemasFetched[1].Key())

		schemasMap := make(map[string]domain.Schema)
		for _, schema := range schemasFetched {
			schemasMap[schema.Key()] = schema
		}

		schema0Fetched := schemasMap[sc0.Key()]
		assert.Equal(t, sc0.ID(), schema0Fetched.ID())
		assert.Equal(t, sc0.Schema(), schema0Fetched.Schema())
		assert.Equal(t, sc0.Version(), schema0Fetched.Version())
		assert.Equal(t, true, slices.Equal(sc0.Fields(), schema0Fetched.Fields()))

		schema1Fetched := schemasMap[sc1.Key()]
		assert.Equal(t, sc1.ID(), schema1Fetched.ID())
		assert.Equal(t, sc1.Schema(), schema1Fetched.Schema())
		assert.Equal(t, sc1.Version(), schema1Fetched.Version())
		assert.Equal(t, true, slices.Equal(sc1.Fields(), schema1Fetched.Fields()))

		cleanupRedis(schemasFetched)
	})
}
