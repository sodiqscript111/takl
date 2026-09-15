package model

type EventType string

const (
	EventRunnerJoined   EventType = "RunnerJoined"
	EventRunnerLeft     EventType = "RunnerLeft"
	EventRunnerDraining EventType = "RunnerDraining"
)

type Event struct {
	Seq      int64          `json:"seq"`
	Type     EventType      `json:"type"`
	RunnerID string         `json:"runner_id"`
	Payload  map[string]any `json:"payload,omitempty"`
	HLC      HLC            `json:"hlc"`
}
