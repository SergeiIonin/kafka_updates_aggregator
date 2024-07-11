package test_fieldscache

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go"
	tcWait "github.com/testcontainers/testcontainers-go/wait"
	"kafka_updates_aggregator/kafka_aggregator/fieldscache"
	"kafka_updates_aggregator/test"
	"log"
	"testing"
	"time"
)

var (
	redisAddr      string
	redisContainer tc.Container
	ctx            context.Context
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
}

func TestFieldsRedisCache_test(t *testing.T) {
	defer func(ctx context.Context, container tc.Container, t *testing.T) {
		err := test.TerminateTestContainer(ctx, container)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}(ctx, redisContainer, t)

	cache := fieldscache.NewFieldsRedisCache(redisAddr)

	t.Run("insert, get and update fields in cache", func(t *testing.T) {
		userId := "bob"
		err := cache.Upsert(ctx, userId, "balance", 100)
		assert.NoError(t, err)

		err = cache.Upsert(ctx, userId, "deposit", 200)
		assert.NoError(t, err)

		balance, err := cache.Get(ctx, userId, "balance")
		assert.NoError(t, err)
		assert.Equal(t, "100", balance)

		deposit, err := cache.Get(ctx, userId, "deposit")
		assert.NoError(t, err)
		assert.Equal(t, "200", deposit)

		err = cache.Upsert(ctx, userId, "balance", 150)
		assert.NoError(t, err)

		balance, err = cache.Get(ctx, userId, "balance")
		assert.NoError(t, err)
		assert.Equal(t, "150", balance)
	})

}
