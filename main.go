package main

import (
	"fmt"
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

	port := "9854"

	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}

	err := r.Run(fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		panic(err)
	}
}
