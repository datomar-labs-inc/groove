package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	groove "github.com/datomar-labs-inc/groove/common"
)

func hEnqueue(c *gin.Context) {
	var input groove.EnqueueTaskInput

	err := c.ShouldBindJSON(&input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(input.Tasks) > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot enqueue more than 1000 tasks"})
		return
	}

	wait := c.Query("wait") == "true"

	var fails int
	var successes int

	var tasks []groove.Task

	if wait {
		waits := grooveMaster.EnqueueAndWait(input.Tasks)

		for _, w := range waits {
			task := <-w

			if task.Succeeded {
				successes++
			} else {
				fails++
			}

			tasks = append(tasks, task)
		}
	} else {
		grooveMaster.Enqueue(input.Tasks)
	}

	resp := gin.H{"status": "ok"}

	if wait {
		resp["processed"] = successes
		resp["failed"] = fails
		resp["tasks"] = tasks

		if fails > 0 {
			resp["status"] = "has_failures"
		} else {
			resp["status"] = "processed"
		}
	} else {
		resp["enqueued"] = len(input.Tasks)
		resp["status"] = "processed"
	}

	c.JSON(http.StatusOK, resp)
}

func hDequeue(c *gin.Context) {
	var input groove.DequeueTaskInput

	err := c.ShouldBindJSON(&input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if input.DesiredTaskCount > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot dequeue more than 1000 tasks"})
		return
	}

	taskSet := grooveMaster.Dequeue(input.DesiredTaskCount, input.Prefix, time.Duration(input.Timeout)*time.Millisecond)

	// A task set could not be formed due to not enough tasks
	if taskSet == nil {
		c.JSON(http.StatusOK, gin.H{
			"status": "no_tasks_available",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "ok",
		"task_set": taskSet,
	})
}

func hAck(c *gin.Context) {
	var input groove.AckInput

	err := c.ShouldBindJSON(&input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if input.TaskID != nil {
		err = grooveMaster.AckTask(input.TaskSetID, *input.TaskID, input.Result)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		err = grooveMaster.Ack(input.TaskSetID, input.Result)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func hNack(c *gin.Context) {
	var input groove.AckInput

	err := c.ShouldBindJSON(&input)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if input.TaskID != nil {
		err = grooveMaster.NackTask(input.TaskSetID, *input.TaskID, input.Error)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		err = grooveMaster.Nack(input.TaskSetID, input.Error)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
