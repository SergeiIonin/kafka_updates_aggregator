package test

import (
	"context"
	"log"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/redis/go-redis/v9"
)

func waitRedisIsUp() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	for {
		select {
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		default:
			log.Println("WAITING FOR REDIS TO BE READY...")
			err := redisClient.Ping(ctx).Err()
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			cancel()
			return nil
		}
	}
}

func CreateRedisContainer(dockerClient *client.Client) (id string, err error) {
	ctx := context.Background()

	config := &container.Config{
		Image: "redis:latest",
		ExposedPorts: nat.PortSet{
			"6379": struct{}{},
		},
		Tty: false,
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"6379": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "6379",
				},
			},
		},
	}

	resp, err := dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, "redis")
	if err != nil {
		panic(err)
	}

	if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		panic(err)
	}

	log.Println("WAITING FOR REDIS CONTAINER TO START...")

	if err = waitRedisIsUp(); err != nil {
		panic(err)
	}

	return resp.ID, nil
}
