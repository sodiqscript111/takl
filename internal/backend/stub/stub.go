package stub

import (
	"time"

	"takl/internal/model"
)

type Config struct {
	NodeID		string
	Hostname	string
	IP		string
	Region		string
	Version		string
	Capacity	int
}

type Stub struct {
	config Config
}

func New(cfg Config) *Stub {
	return &Stub{
		config: cfg,
	}
}

func (s *Stub) Snapshot() model.Runner {
	return model.Runner{
		RunnerID:		s.config.NodeID,
		Hostname:		s.config.Hostname,
		IP:			s.config.IP,
		Region:			s.config.Region,
		Version:		s.config.Version,
		Status:			model.RunnerActive,
		LastHeartbeat:		time.Now().Unix(),
		CPUUtil:		0.05,
		MemUtil:		0.10,
		FreeDiskMB:		50000,
		BuildCacheMB:		0,
		WorkerCapacity:		s.config.Capacity,
		AvailableWorkers:	s.config.Capacity,
		ActiveBuilds:		0,
		QueueDepth:		0,
		Containers:		0,
		Mounts:			0,
		Labels:			map[string]string{"type": "stub"},
	}
}

func (s *Stub) Step()	{}

func (s *Stub) DrainEvents() []model.Event {
	return nil
}

func (s *Stub) Builds() []model.Build {
	return nil
}

func (s *Stub) Queues() []model.QueueStats {
	return nil
}

func (s *Stub) Containers() []model.Container {
	return nil
}

func (s *Stub) Mounts() []model.Mount {
	return nil
}

func (s *Stub) Images() []model.Image {
	return nil
}
