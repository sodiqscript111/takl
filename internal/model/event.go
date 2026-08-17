package model

type EventType string

const (
	EventRunnerJoined      EventType = "RunnerJoined"
	EventRunnerLeft        EventType = "RunnerLeft"
	EventRunnerDraining    EventType = "RunnerDraining"
	EventBuildStarted      EventType = "BuildStarted"
	EventBuildFinished     EventType = "BuildFinished"
	EventBuildFailed       EventType = "BuildFailed"
	EventQueueDepthChanged EventType = "QueueDepthChanged"
	EventContainerCreated  EventType = "ContainerCreated"
	EventContainerDeleted  EventType = "ContainerDeleted"
	EventMountCreated      EventType = "MountCreated"
	EventMountRemoved      EventType = "MountRemoved"
	EventDeployStarted     EventType = "DeployStarted"
	EventDeployCompleted   EventType = "DeployCompleted"
	EventDeployFailed      EventType = "DeployFailed"
	EventDeployCancelled   EventType = "DeployCancelled"
	EventServiceRegistered EventType = "ServiceRegistered"
	EventImageDiscovered   EventType = "ImageDiscovered"
	EventImagePruned       EventType = "ImagePruned"
)

type Event struct {
	Seq      int64          `json:"seq"`
	Type     EventType      `json:"type"`
	RunnerID string         `json:"runner_id"`
	Payload  map[string]any `json:"payload,omitempty"`
	HLC      HLC            `json:"hlc"`
}
