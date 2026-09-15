package stub

import (
	"os"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"

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

type Stub struct {
	config Config
}

func New(cfg Config) *Stub {
	return &Stub{
		config: cfg,
	}
}

func (s *Stub) Snapshot() model.Runner {
	memUtil := 0.0
	if v, err := mem.VirtualMemory(); err == nil {
		memUtil = v.UsedPercent / 100.0
	}

	cpuUtil := 0.0
	if c, err := cpu.Percent(0, false); err == nil && len(c) > 0 {
		cpuUtil = c[0] / 100.0
	}

	freeDiskMB := int64(0)
	path := "/"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = "."
	}
	if d, err := disk.Usage(path); err == nil {
		freeDiskMB = int64(d.Free / 1024 / 1024)
	}

	return model.Runner{
		RunnerID:         s.config.NodeID,
		Hostname:         s.config.Hostname,
		IP:               s.config.IP,
		Region:           s.config.Region,
		Version:          s.config.Version,
		Status:           model.RunnerActive,
		LastHeartbeat:    time.Now().Unix(),
		CPUUtil:          cpuUtil,
		MemUtil:          memUtil,
		FreeDiskMB:       freeDiskMB,
		WorkerCapacity:   s.config.Capacity,
		AvailableWorkers: s.config.Capacity,
		Labels:           map[string]string{},
	}
}

func (s *Stub) Step() {}
