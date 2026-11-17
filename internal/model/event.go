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
	// Origin is the store id of the node that emitted the event. Together with
	// RunnerID and Seq it identifies the event, so a node that restarts with a
	// fresh database (new store id, seq starting over) never collides.
	Origin string `json:"origin,omitempty"`
}
