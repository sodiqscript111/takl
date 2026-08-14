package docker

import (
	"context"
	"log/slog"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"

	"takl/internal/model"
)

type Config struct {
	NodeID   string
	Hostname string
	IP       string
	Region   string
	Version  string
	Capacity int
}

type Backend struct {
	config Config
	cli    *client.Client
}

func New(cfg Config) (*Backend, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	return &Backend{
		config: cfg,
		cli:    cli,
	}, nil
}

func (b *Backend) Snapshot() model.Runner {
	return model.Runner{
		RunnerID:         b.config.NodeID,
		Hostname:         b.config.Hostname,
		IP:               b.config.IP,
		Region:           b.config.Region,
		Version:          b.config.Version,
		Status:           model.RunnerActive,
		LastHeartbeat:    time.Now().Unix(),
		CPUUtil:          0.05,
		MemUtil:          0.10,
		FreeDiskMB:       50000,
		BuildCacheMB:     0,
		WorkerCapacity:   b.config.Capacity,
		AvailableWorkers: b.config.Capacity,
		ActiveBuilds:     0,
		QueueDepth:       0,
		Containers:       0,
		Mounts:           0,
		Labels:           map[string]string{"type": "docker"},
	}
}

func (b *Backend) Step() {}

func (b *Backend) DrainEvents() []model.Event {
	return nil
}

func (b *Backend) Builds() []model.Build {
	return nil
}

func (b *Backend) Queues() []model.QueueStats {
	return nil
}

func (b *Backend) Containers() []model.Container {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	containers, err := b.cli.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil {
		slog.Error("docker list containers", "err", err)
		return nil
	}

	var res []model.Container
	for _, c := range containers {
		res = append(res, model.Container{
			ContainerID: c.ID,
			RunnerID:    b.config.NodeID,
			ProjectID:   "none",
			Image:       c.Image,
			Status:      c.State,
			CreatedAt:   c.Created,
		})
	}
	return res
}

func (b *Backend) Mounts() []model.Mount {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	vols, err := b.cli.VolumeList(ctx, volume.ListOptions{})
	if err != nil {
		slog.Error("docker list volumes", "err", err)
		return nil
	}

	var res []model.Mount
	for _, v := range vols.Volumes {
		res = append(res, model.Mount{
			RunnerID: b.config.NodeID,
			ProjectID: "none",
			MountPath:   v.Mountpoint,
			LastAccessed: time.Now().Unix(),
		})
	}
	return res
}

func (b *Backend) Images() []model.Image {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	imgs, err := b.cli.ImageList(ctx, types.ImageListOptions{})
	if err != nil {
		slog.Error("docker list images", "err", err)
		return nil
	}

	var res []model.Image
	for _, img := range imgs {
		repoTags := img.RepoTags
		name := ""
		if len(repoTags) > 0 {
			name = repoTags[0]
		} else {
			name = img.ID
		}
		res = append(res, model.Image{
			ImageID:  img.ID,
			Digest:   "",
			Repository: name,
			Tag:      "",
			SizeBytes:   img.Size,
			RunnerID: b.config.NodeID,
			LastSeen: time.Now().Unix(),
		})
	}
	return res
}
