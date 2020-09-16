package groove

import (
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	gonanoid "github.com/matoous/go-nanoid"
	"github.com/vmihailenco/msgpack/v4"
	"sync"
	"time"
)

const WRITE_BUFFER_SIZE = 64

type TaskHandler func(task *Task) (res interface{}, err error)

type Sub struct {
	Subscription
	handler TaskHandler
}

type WSClient struct {
	wsConn *websocket.Conn
	reqs   map[string]chan Message

	subscriptions []Sub

	mx     sync.Mutex
	writer chan Message
}

func NewWSClient() *WSClient {
	return &WSClient{
		wsConn:        nil,
		reqs:          map[string]chan Message{},
	}
}

func (w *WSClient) Connect(addr string) error {
	c, _, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		return err
	}

	w.wsConn = c

	w.writer = make(chan Message, WRITE_BUFFER_SIZE)

	// Start a goroutine to read messages
	go func() {
		for {
			_, m, err := c.ReadMessage()
			if err != nil {
				fmt.Println("read err", err)
			} else {
				var msg Message

				err := msgpack.Unmarshal(m, &msg)
				if err != nil {
					fmt.Println("unmarshal err", err)
				} else {
					w.handleMessage(&msg)
				}
			}
		}
	}()

	// Start a goroutine to write messages to the connection
	go func() {
		for m := range w.writer {
			mb, err := msgpack.Marshal(m)
			if err != nil {
				fmt.Println("Failed to marshal message", err)
				continue
			}

			w.mx.Lock()
			err = w.wsConn.WriteMessage(websocket.BinaryMessage, mb)
			w.mx.Unlock()
			if err != nil {
				fmt.Println("Failed to write message", err)
			}
		}
	}()

	return nil
}

func (w *WSClient) handleMessage(msg *Message) {
	// If this message is in response to a previous message
	if ch, ok := w.reqs[msg.ID]; ok {
		ch <- *msg
		delete(w.reqs, msg.ID)
	} else {
		fmt.Printf("%+v\n", msg)
	}
}

func (w *WSClient) Disconnect() error {
	if w.wsConn == nil {
		return errors.New("client not connected")
	}

	return w.wsConn.Close()
}

func (w *WSClient) reqres(msg *Message, timeout time.Duration) (*Message, error) {
	msg.ID, _ = gonanoid.Nanoid()

	ch := make(chan Message)
	w.reqs[msg.ID] = ch
	w.writer <- *msg

	select {
	case res := <-ch:
		return &res, nil
	case <-time.After(timeout):
		return nil, errors.New("request timed out")
	}
}

func (w *WSClient) Subscribe(prefix string, capacity int, handler TaskHandler) error {
	if w.wsConn == nil {
		return errors.New("client not connected")
	}

	msg := Subscription{
		Capacity: capacity,
	}

	if prefix != "" {
		msg.PrefixSelector = &prefix
	}

	w.subscriptions = append(w.subscriptions, Sub{
		Subscription: msg,
		handler:      handler,
	})

	_, err := w.reqres(&Message{Type: MessageTypeSubscription, Subscription: &msg}, 5*time.Second)
	if err != nil {
		return err
	}

	return nil
}

func (w *WSClient) Enqueue(tasks []Task) error {
	if w.wsConn == nil {
		return errors.New("client not connected")
	}

	_, err := w.reqres(&Message{Type: MessageTypeEnqueue, Tasks: tasks}, 5*time.Second)
	if err != nil {
		return err
	}

	return nil
}
