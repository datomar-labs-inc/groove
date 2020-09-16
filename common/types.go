package groove

import (
	"time"
)

const (
	MessageTypeSubscription = iota
	MessageTypeTask
	MessageTypeError
	MessageTypeEnqueue
	MessageTypeMisc
)

// Message the struct that gets passed back and forth in websocket communications
type Message struct {
	Type         int           `json:"type" msgpack:"t"`
	ID           string        `json:"id" msgpack:"i"`
	Error        *string       `json:"error,omitempty" msgpack:"e,omitempty"`
	Task         *Task         `json:"task,omitempty" msgpack:"k,omitempty"`
	Tasks        []Task        `json:"tasks,omitempty" msgpack:"z,omitempty"`
	Subscription *Subscription `json:"subscription,omitempty" msgpack:"s,omitempty"`
	Ack          *AckInput     `json:"ack,omitempty" msgpack:"a,omitempty"`
	Misc         interface{}   `json:"misc,omitempty" msgpack:"m,omitempty"`
}

type Subscription struct {
	PrefixSelector *string `json:"prefix_selector,omitempty" msgpack:"p,omitempty"`
	Capacity       int     `json:"capacity" msgpack:"c"` // How many concurrent messages this subscription can process
}

// Task is a single bit of work that can be done
type Task struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`

	Succeeded      bool          `json:"succeeded"`
	RetryThreshold int           `json:"retry_threshold"` // How many times the task will be retried before being marked as a failure
	Errors         []interface{} `json:"errors,omitempty"`
	Result         interface{}   `json:"result,omitempty"`
	RetryCount     int           `json:"-"`
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
	TaskSetID string      `json:"task_set_id"`
	TaskID    *string     `json:"task_id,omitempty"`
	Error     interface{} `json:"error,omitempty"`
	Result    interface{} `json:"result,omitempty"`
}
