package main

import (
	"net/http"

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

	grooveMaster.Enqueue(input.Tasks)

	c.JSON(http.StatusOK, gin.H{"enqueued": len(input.Tasks), "status": "ok"})
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

	taskSet := grooveMaster.Dequeue(input.DesiredTaskCount, input.Prefix)

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
		err = grooveMaster.AckTask(input.TaskSetID, *input.TaskID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		err = grooveMaster.Ack(input.TaskSetID)
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
		err = grooveMaster.NackTask(input.TaskSetID, *input.TaskID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		err = grooveMaster.Nack(input.TaskSetID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
