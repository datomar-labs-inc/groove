package main

import (
	"fmt"
	"testing"
)

func TestGrooveQueue_Enqueue(t *testing.T) {
	gq := NewGrooveQueue()

	done := false

	// Start producer
	go func() {
		for j := 0; j < 20; j++ {
			var tasks []*Task

			for i := 0; i < 500; i++ {
				reqs := RequirementSet{}

				if i%3 == 0 {
					reqs["rare_resource"] = Requirements{WriteAccess: true}
				}

				if i%2 == 0 {
					reqs["std_resource"] = Requirements{WriteAccess: true}
				}

				if i%5 == 0 {
					reqs["common_resource"] = Requirements{WriteAccess: true}
				} else {
					reqs["common_resource"] = Requirements{WriteAccess: false}
				}

				tasks = append(tasks, &Task{
					ID:       fmt.Sprintf("task:%d:%d", j, i),
					SortKey:  uint64(i * j),
					Requires: reqs,
				})
			}

			gq.Enqueue(tasks)
		}
		done = true
	}()

	for {
		var ids []string

		n := 0
		task := gq.getFirstAvailableTask()
		if task == nil {
			continue
		}

		for {
			n++

			if n != 0 {
				fmt.Println(task)
			} else {
				fmt.Println("DOUBLE TASK", task)
			}

			ids = append(ids, task.ID)

			task = gq.getFirstAvailableTask()
			if task == nil {
				break
			}
		}

		for _, id := range ids {
			err := gq.Ack(id, nil)
			if err != nil {
				t.Error(err)
			}
		}

		if len(gq.tasks) == 0 && done {
			break
		}
	}
}
