package groove

import (
	"time"
)

// Task is a single bit of work that can be done
type Task struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`

	RetryThreshold int `json:"retry_threshold"` // How many times the task will be retried before being marked as a failure
	RetryCount     int `json:"-"`
}

// TaskSetLog keeps track of a task set, noting which tasks are included in it
type TaskSetLog struct {
	ID        string    `json:"id"`
	TaskIDs   []string  `json:"task_ids"`
	TimeoutAt time.Time `json:"timeout_at"`
}

// TaskSet is a group of tasks that should be processed at once
type TaskSet struct {
	ID    string `json:"id"`
	Tasks []Task `json:"tasks"`
}

type EnqueueTaskInput struct {
	Tasks []Task `json:"tasks"`
}

type DequeueTaskInput struct {
	DesiredTaskCount int    `json:"desired_task_count"`
	Prefix           string `json:"prefix"`
	Timeout          int    `json:"timeout"` // Number of milliseconds that groove should wait before declaring your tasks failed
}

type AckInput struct {
	TaskSetID string  `json:"task_set_id"`
	TaskID    *string `json:"task_id,omitempty"`
}
