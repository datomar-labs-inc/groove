package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	groove "github.com/datomar-labs-inc/groove/common"
)

type GrooveMaster struct {
	mx sync.Mutex

	RootContainer *TaskContainer
	TaskSetLogs   map[string]groove.TaskSetLog
	Waits         map[string][]chan bool
}

func New() *GrooveMaster {
	return &GrooveMaster{
		TaskSetLogs: map[string]groove.TaskSetLog{},
		RootContainer: &TaskContainer{
			Children: map[string]*TaskContainer{},
			Tasks:    nil,
		},
		Waits: map[string][]chan bool{},
	}
}

func (g *GrooveMaster) Print() {
	fmt.Print(g.RootContainer.String())
}

func (g *GrooveMaster) Enqueue(tasks []groove.Task) {
	g.mx.Lock()
	defer g.mx.Unlock()

	for _, t := range tasks {
		g.putTask(t)
	}
}

func (g *GrooveMaster) EnqueueAndWait(tasks []groove.Task) []chan bool {
	g.mx.Lock()
	defer g.mx.Unlock()

	var waits []chan bool

	for _, t := range tasks {
		g.putTask(t)
		waits = append(waits, g.putWait(t.ID))
	}

	return waits
}

// Ack is used to acknowledge that all work in a TaskSet has been completed
func (g *GrooveMaster) Ack(taskSetID string) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	// Load the task set log
	ts, ok := g.TaskSetLogs[taskSetID]
	if ok {

		// Update task containers for each task
		for _, taskID := range ts.TaskIDs {
			idParts := strings.Split(taskID, ".")

			// Find the TaskContainer that contains the current task
			cc, key := g.RootContainer.GetChildContainer(strings.Join(idParts[:len(idParts)-1], "."))
			if cc != nil {
				if cc.Locked && cc.LockedTask.ID == taskID {
					// Check for waits and complete them
					if waits, ok := g.Waits[taskID]; ok {
						for _, w := range waits {
							w <- true
						}

						delete(g.Waits, taskID)
					}

					cc.LockedTask = nil
					cc.Locked = false

					// remove the TaskContainer from the tree if it has no more tasks
					if len(cc.Tasks) == 0 {
						delete(cc.Parent.Children, key)
					}
				} else {
					return errors.New("task set was not locked")
				}
			}
		}
	} else {
		return errors.New("task set did not exist")
	}

	return nil
}

// Nack is used to acknowledge that all work in a TaskSet has failed
func (g *GrooveMaster) Nack(taskSetID string) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	// Load the task set log
	ts, ok := g.TaskSetLogs[taskSetID]
	if ok {

		// Update task containers for each task
		for _, taskID := range ts.TaskIDs {
			idParts := strings.Split(taskID, ".")

			// Find the TaskContainer that contains the current task
			cc, _ := g.RootContainer.GetChildContainer(strings.Join(idParts[:len(idParts)-1], "."))
			if cc != nil {
				if cc.Locked && cc.LockedTask.ID == taskID {
					cc.Tasks = append([]groove.Task{*cc.LockedTask}, cc.Tasks...)
					cc.LockedTask = nil
					cc.Locked = false
				} else {
					return errors.New("task set was not locked")
				}
			}
		}

		// Remove task set log
		delete(g.TaskSetLogs, taskSetID)
	} else {
		return errors.New("task set did not exist")
	}

	return nil
}

// NackTask is used to note that a single task in a task set has failed
func (g *GrooveMaster) NackTask(taskSetID string, failedTaskID string) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	// Load the task set log
	ts, ok := g.TaskSetLogs[taskSetID]
	if ok {

		nacked := false

		// Update task containers for each task
		for i, taskID := range ts.TaskIDs {
			if taskID == failedTaskID {
				idParts := strings.Split(taskID, ".")

				// Find the TaskContainer that contains the current task
				cc, _ := g.RootContainer.GetChildContainer(strings.Join(idParts[:len(idParts)-1], "."))
				if cc != nil {
					if cc.Locked && cc.LockedTask.ID == taskID {
						// Add task back to front of list
						cc.Tasks = append([]groove.Task{*cc.LockedTask}, cc.Tasks...)
						cc.LockedTask = nil
						cc.Locked = false

						// Remove task from TaskSet
						ts.TaskIDs = append(ts.TaskIDs[:i], ts.TaskIDs[i+1:]...)
					} else {
						return errors.New("task set was not locked")
					}
				}

				nacked = true

				break
			}
		}

		if !nacked {
			return errors.New("task did not exist")
		}
	} else {
		return errors.New("task set did not exist")
	}

	return nil
}

// AckTask is used to note that a single task in a task set has been completed
func (g *GrooveMaster) AckTask(taskSetID string, succeededTaskID string) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	// Load the task set log
	ts, ok := g.TaskSetLogs[taskSetID]
	if ok {
		acked := false

		// Update task containers for each task
		for i, taskID := range ts.TaskIDs {
			if taskID == succeededTaskID {
				idParts := strings.Split(taskID, ".")

				// Find the TaskContainer that contains the current task
				cc, _ := g.RootContainer.GetChildContainer(strings.Join(idParts[:len(idParts)-1], "."))
				if cc != nil {
					if cc.Locked && cc.LockedTask.ID == taskID {
						// Check for waits and complete them
						if waits, ok := g.Waits[taskID]; ok {
							for _, w := range waits {
								w <- true
							}

							delete(g.Waits, taskID)
						}

						// Add task back to front of list
						cc.LockedTask = nil
						cc.Locked = false

						// Remove task from TaskSet
						ts.TaskIDs = append(ts.TaskIDs[:i], ts.TaskIDs[i+1:]...)
					} else {
						return errors.New("task set was not locked")
					}
				}

				acked = true

				break
			}
		}

		if !acked {
			return errors.New("task did not exist")
		}

		// Remove the task set if there are no more tasks
		if len(ts.TaskIDs) == 0 {
			delete(g.TaskSetLogs, taskSetID)
		}
	} else {
		return errors.New("task set did not exist")
	}

	return nil
}

func (g *GrooveMaster) Dequeue(desiredTasks int, prefix string) *groove.TaskSet {
	g.mx.Lock()
	defer g.mx.Unlock()

	var tasks []groove.Task
	var taskIDs []string

	var tc *TaskContainer

	if prefix != "" {
		tc, _ = g.RootContainer.GetChildContainer(prefix)
	} else {
		tc = g.RootContainer
	}

	if tc == nil {
		return nil
	}

	for {
		task := tc.TreePop()

		if task != nil {
			tasks = append(tasks, *task)
			taskIDs = append(taskIDs, task.ID)
		} else {
			break
		}

		if len(tasks) >= desiredTasks {
			break
		}
	}

	// Don't create a task set if there are no tasks
	if len(tasks) == 0 {
		return nil
	}

	id := uuid.Must(uuid.NewRandom()).String()

	ts := groove.TaskSet{
		ID:    id,
		Tasks: tasks,
	}

	tsl := groove.TaskSetLog{
		ID:      id,
		TaskIDs: taskIDs,
	}

	g.TaskSetLogs[id] = tsl

	return &ts
}

// putTask is not safe to be called on it's own. The caller must ensure thread safety
func (g *GrooveMaster) putTask(task groove.Task) {
	idParts := strings.Split(task.ID, ".")

	// Check that the id has 2 or more parts, since the last part does not get grooved
	if len(idParts) > 1 {
		// Verify all maps are in place in the tree
		var tc = g.RootContainer

		for i, IDp := range idParts {
			if i != len(idParts)-1 {
				tcn, ok := tc.Children[IDp]
				if !ok {
					newTaskContainer := &TaskContainer{
						Parent:   tc,
						Children: map[string]*TaskContainer{},
						Tasks:    nil,
					}

					tc.Children[IDp] = newTaskContainer

					tcn = newTaskContainer
				}

				tc = tcn
			} else {
				tc.Tasks = append(tc.Tasks, task)
			}
		}
	}
}

// putWait is not safe to be called on it's own. The caller must ensure thread safety
func (g *GrooveMaster) putWait(taskID string) chan bool {
	ch := make(chan bool, 1)
	g.Waits[taskID] = append(g.Waits[taskID], ch)
	return ch
}

type TaskContainer struct {
	Locked     bool         `json:"locked"`
	LockedTask *groove.Task `json:"locked_task"` // The task which is currently being processed

	Parent *TaskContainer `json:"-"`

	Children map[string]*TaskContainer `json:"children"`
	Tasks    []groove.Task             `json:"tasks"`
}

func (t *TaskContainer) String() string {
	var str string

	if len(t.Tasks) > 0 {
		str = fmt.Sprintf("\n	) %d tasks\n", len(t.Tasks))
	}

	for k, v := range t.Children {
		str += fmt.Sprintf(".%s\n%s", k, v.String())
	}

	return str
}

func (t *TaskContainer) TreePop() (task *groove.Task) {
	if len(t.Tasks) > 0 && !t.Locked {
		task := t.Pop()
		t.LockedTask = &task
		t.Locked = true
		return &task
	}

	for _, v := range t.Children {
		ctp := v.TreePop()

		if ctp != nil {
			return ctp
		}
	}

	return nil
}

func (t *TaskContainer) Pop() (task groove.Task) {
	task, t.Tasks = t.Tasks[0], t.Tasks[1:]
	return task
}

func (t *TaskContainer) GetChildContainer(id string) (*TaskContainer, string) {
	idParts := strings.Split(id, ".")

	var tc = t
	var idPart string

	for _, idP := range idParts {
		tcn, ok := tc.Children[idP]
		if !ok {
			return nil, ""
		}

		tc = tcn
		idPart = idP
	}

	return tc, idPart
}
