package main

import (
	"errors"
	"sort"
	"sync"
)

const (
	RESOURCE_WRITE_LOCKED = 0
)

// Task is a task to be completed
type Task struct {
	ID       string         `json:"id"`
	SortKey  uint64         `json:"sort_key"` // Tasks will be sorted by sort key, lowest first
	Data     interface{}    `json:"data"`
	Result   interface{}    `json:"result"` // Result gets set when the task is finished
	Requires RequirementSet `json:"requires"`
}

type RequirementSet map[string]Requirements

type Requirements struct {
	WriteAccess bool `json:"write"`
}

// GrooveQueue arranges tasks into a tree structure, so tasks higher up the tree must be completed
// before lower tasks can be started
type GrooveQueue struct {
	mx          sync.Mutex
	tasks       []*Task
	resources   map[string]uint16
	activeTasks map[string]*Task
}

func (gq *GrooveQueue) Enqueue(tasks []*Task) {
	gq.mx.Lock()
	defer gq.mx.Unlock()

	for _ , task := range tasks {
		idx := sort.Search(len(gq.tasks), func(i int) bool {
			return gq.tasks[i].SortKey >= task.SortKey
		})

		gq.tasks = append(gq.tasks, task)
		copy(gq.tasks[idx+1:], gq.tasks[idx:])
		gq.tasks[idx] = task
	}
}

func (gq *GrooveQueue) Ack(id string, result interface{}) error {
	gq.mx.Lock()
	defer gq.mx.Unlock()

	if task, ok := gq.activeTasks[id]; ok {
		task.Result = result

		for resourceID := range task.Requires {
			if resource, ok := gq.resources[resourceID]; ok {
				if resource != RESOURCE_WRITE_LOCKED {
					gq.resources[resourceID]--
				}

				if gq.resources[resourceID] == 0 {
					delete(gq.resources, resourceID)
				}
			}
		}

		delete(gq.activeTasks, id)
	} else {
		return errors.New("task did not exist")
	}

	return nil
}

func (gq *GrooveQueue) requirementsSatisfied(requirements RequirementSet) bool {
	if requirements == nil {
		return false
	}

	for resourceID, requirement := range requirements {

		// In order to be usable, the resource must satisfy one of the following conditions
		// Not existing (ie. not locked)
		// Existing, but only read locked (and requirement only requires read lock)
		if resource, ok := gq.resources[resourceID]; !ok || (ok && resource != RESOURCE_WRITE_LOCKED && requirement.WriteAccess == false) {
			// The condition is satisfied, do nothing
		} else {
			return false
		}
	}

	return true
}

func (gq *GrooveQueue) getFirstAvailableTask() *Task {
	gq.mx.Lock()
	defer gq.mx.Unlock()

	var task *Task

	for i, t := range gq.tasks {
		if gq.requirementsSatisfied(t.Requires) {
			task = t

			copy(gq.tasks[i:], gq.tasks[i+1:])
			gq.tasks[len(gq.tasks)-1] = nil // or the zero value of T
			gq.tasks = gq.tasks[:len(gq.tasks)-1]
			break
		}
	}

	if task != nil {
		gq.activeTasks[task.ID] = task

		for resourceID, requirement := range task.Requires {
			if requirement.WriteAccess {
				gq.resources[resourceID] = RESOURCE_WRITE_LOCKED
			} else {
				gq.resources[resourceID]++
			}
		}

		return task
	}

	return nil
}

func NewGrooveQueue() *GrooveQueue {
	return &GrooveQueue{
		resources:   map[string]uint16{},
		activeTasks: map[string]*Task{},
		tasks:       []*Task{},
	}
}
