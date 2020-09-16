package groove

import (
	"sync"
	"testing"
)

func TestWSClient_Connect(t *testing.T) {
	client := NewWSClient()
	err := client.Connect("ws://localhost:9854/ws")
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	err = client.Subscribe("", 5, func(task *Task) (res interface{}, err error) {
		defer wg.Done()

		return nil, nil
	})
	if err != nil {
		t.Error(err)
		return
	}

	wg.Wait()
}

func TestWSClient_Disconnect(t *testing.T) {

}

func TestWSClient_Subscribe(t *testing.T) {

}