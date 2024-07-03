package testutils

import (
	"context"
	"errors"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	tc "github.com/testcontainers/testcontainers-go"
)

func TerminateTestContainer(container tc.Container, ctx context.Context) error {
	if err := container.Terminate(ctx); err != nil {
		return fmt.Errorf("error terminating testcontainer %v", err)
	}
	return nil
}

func TerminateContainer(dockerClient *client.Client, containerId string) error {
	ctx := context.Background()
	errs := make([]error, 0, 2)
	err := dockerClient.ContainerKill(ctx, containerId, "SIGKILL")
	if err != nil {
		err = fmt.Errorf("error killing container %s: %v", containerId, err)
		errs = append(errs, err)
	}
	err = dockerClient.ContainerRemove(ctx, containerId, container.RemoveOptions{Force: true})
	if err != nil {
		err = fmt.Errorf("error removing container %s: %v", containerId, err)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
