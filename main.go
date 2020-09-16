package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

var grooveMaster *GrooveMaster

func main() {
	grooveMaster = New()

	r := gin.Default()

	r.POST("/dequeue", hDequeue)
	r.POST("/enqueue", hEnqueue)
	r.POST("/ack", hAck)
	r.POST("/nack", hNack)

	r.GET("/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": grooveMaster.RootContainer.String()})
	})

	r.GET("/data", func(c *gin.Context) {
		grooveMaster.mx.Lock()
		defer grooveMaster.mx.Unlock()

		c.JSON(http.StatusOK, grooveMaster.RootContainer)
	})

	r.GET("/ws", func(c *gin.Context) {
		grooveMaster.handleWS(c.Writer, c.Request)
	})

	port := "9854"

	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}

	err := r.Run(fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		panic(err)
	}
}
