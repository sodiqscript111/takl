package model

import "encoding/json"

type RunnerStatus string

const (
	RunnerActive	RunnerStatus	= "active"
	RunnerDraining	RunnerStatus	= "draining"
	RunnerOffline	RunnerStatus	= "offline"
)

type BuildStatus string

const (
	BuildQueued	BuildStatus	= "queued"
	BuildBuilding	BuildStatus	= "building"
	BuildPushing	BuildStatus	= "pushing"
	BuildCleaning	BuildStatus	= "cleaning"
	BuildFinished	BuildStatus	= "finished"
	BuildFailed	BuildStatus	= "failed"
)

type Runner struct {
	RunnerID		string			`json:"runner_id"`
	Hostname		string			`json:"hostname"`
	IP			string			`json:"ip"`
	Region			string			`json:"region"`
	Version			string			`json:"version"`
	Status			RunnerStatus		`json:"status"`
	LastHeartbeat		int64			`json:"last_heartbeat"`
	CPUUtil			float64			`json:"cpu_util"`
	MemUtil			float64			`json:"mem_util"`
	FreeDiskMB		int64			`json:"free_disk_mb"`
	BuildCacheMB		int64			`json:"build_cache_mb"`
	WorkerCapacity		int			`json:"worker_capacity"`
	AvailableWorkers	int			`json:"available_workers"`
	ActiveBuilds		int			`json:"active_builds"`
	QueueDepth		int			`json:"queue_depth"`
	Containers		int			`json:"containers"`
	Mounts			int			`json:"mounts"`
	Labels			map[string]string	`json:"labels,omitempty"`
}

func (r Runner) Key() string {
	return r.RunnerID
}

type Build struct {
	BuildID		string		`json:"build_id"`
	ProjectID	string		`json:"project_id"`
	RunnerID	string		`json:"runner_id"`
	Status		BuildStatus	`json:"status"`
	StartedAt	int64		`json:"started_at"`
	UpdatedAt	int64		`json:"updated_at"`
}

func (b Build) Key() string {
	return b.BuildID
}

type QueueStats struct {
	RunnerID	string	`json:"runner_id"`
	QueueName	string	`json:"queue_name"`
	Waiting		int	`json:"waiting"`
	Active		int	`json:"active"`
	Failed		int	`json:"failed"`
	Completed	int	`json:"completed"`
	UpdatedAt	int64	`json:"updated_at"`
}

func (q QueueStats) Key() string {
	return q.RunnerID + "/" + q.QueueName
}

type Container struct {
	ContainerID	string	`json:"container_id"`
	RunnerID	string	`json:"runner_id"`
	ProjectID	string	`json:"project_id"`
	Image		string	`json:"image"`
	Status		string	`json:"status"`
	CreatedAt	int64	`json:"created_at"`
}

func (c Container) Key() string {
	return c.ContainerID
}

type Mount struct {
	RunnerID	string	`json:"runner_id"`
	ProjectID	string	`json:"project_id"`
	MountPath	string	`json:"mount_path"`
	LastAccessed	int64	`json:"last_accessed"`
}

func (m Mount) Key() string {
	return m.RunnerID + "/" + m.MountPath
}

type Image struct {
	ImageID		string	`json:"image_id"`
	Digest		string	`json:"digest"`
	Repository	string	`json:"repository"`
	Tag		string	`json:"tag"`
	SizeBytes	int64	`json:"size_bytes"`
	RunnerID	string	`json:"runner_id"`
	LastSeen	int64	`json:"last_seen"`
}

func (i Image) Key() string {
	return i.ImageID + "/" + i.RunnerID
}

func Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func Decode[T any](b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}
