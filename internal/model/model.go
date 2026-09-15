package model

import "encoding/json"

type RunnerStatus string

const (
	RunnerActive   RunnerStatus = "active"
	RunnerDraining RunnerStatus = "draining"
	RunnerOffline  RunnerStatus = "offline"
)

type Runner struct {
	RunnerID         string            `json:"runner_id"`
	Hostname         string            `json:"hostname"`
	IP               string            `json:"ip"`
	Region           string            `json:"region"`
	Version          string            `json:"version"`
	Status           RunnerStatus      `json:"status"`
	LastHeartbeat    int64             `json:"last_heartbeat"`
	CPUUtil          float64           `json:"cpu_util"`
	MemUtil          float64           `json:"mem_util"`
	FreeDiskMB       int64             `json:"free_disk_mb"`
	WorkerCapacity   int               `json:"worker_capacity"`
	AvailableWorkers int               `json:"available_workers"`
	Labels           map[string]string `json:"labels,omitempty"`
}

func (r Runner) Key() string {
	return r.RunnerID
}

func Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func Decode[T any](b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}
