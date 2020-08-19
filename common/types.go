package groove

// Task is a single bit of work that can be done
type Task struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`
}

// TaskSetLog keeps track of a task set, noting which tasks are included in it
type TaskSetLog struct {
	ID      string   `json:"id"`
	TaskIDs []string `json:"task_ids"`
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
	Timeout          int    `json:"timeout"`
}

type AckInput struct {
	TaskSetID string  `json:"task_set_id"`
	TaskID    *string `json:"task_id,omitempty"`
}
