package testutils

import (
	"context"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	tc "github.com/testcontainers/testcontainers-go"
	"testing"
)

func TerminateTestContainer(container tc.Container, ctx context.Context, t *testing.T) {
	err := container.Terminate(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TerminateContainer(dockerClient *client.Client, containerId string, ctx context.Context, t *testing.T) {
	err := dockerClient.ContainerKill(ctx, containerId, "SIGKILL")
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = dockerClient.ContainerRemove(ctx, containerId, container.RemoveOptions{Force: true})
	if err != nil {
		t.Fatalf(err.Error())
	}
}
