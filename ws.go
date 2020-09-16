package main

import (
	"errors"
	"fmt"
	groove "github.com/datomar-labs-inc/groove/common"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v4"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var wsupgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Connection struct {
	activeTasks   []*groove.Task // A list of tasks this connection is currently working on
	subscriptions []*Subscription
	gm            *GrooveMaster
	active        bool

	writer chan groove.Message // writer writes to the connection
	reader chan groove.Message // reader reads from the connection
}

func NewConnection(gm *GrooveMaster) *Connection {
	conn := &Connection{
		gm:     gm,
		active: true,
		writer: make(chan groove.Message, 5),
		reader: make(chan groove.Message, 5),
	}

	// Start a goroutine to handle incoming messages
	go func() {
		for m := range conn.reader {

			// Handle each message in it's own goroutine
			go func(msg *groove.Message) {

				// Call the message handler for a result or error
				res, err := conn.handleMessage(msg)
				if err != nil {

					// If the message came in with an id, it's expecting a response
					if m.ID != "" {
						e := err.Error()

						resM := groove.Message{
							Type:  groove.MessageTypeError,
							ID:    m.ID,
							Error: &e,
						}

						conn.writer <- resM
					} else {
						fmt.Println("message handler failed", err)
					}
				} else if m.ID != "" {
					res.ID = m.ID
					conn.writer <- *res
				}
			}(&m)
		}
	}()

	return conn
}

func (c *Connection) handleMessage(m *groove.Message) (res *groove.Message, err error) {
	switch m.Type {
	case groove.MessageTypeEnqueue:
		c.gm.Enqueue(m.Tasks)
		return &groove.Message{Type: groove.MessageTypeMisc, Misc: gin.H{"status": "ok"}}, nil
		
	case groove.MessageTypeSubscription:
		sub := &Subscription{
			Subscription: *m.Subscription,
			Connection:   c,
		}

		c.subscriptions = append(c.subscriptions, sub)

		c.gm.wsMX.Lock()
		c.gm.subscriptions = append(c.gm.subscriptions, sub)
		c.gm.wsMX.Unlock()
	case groove.MessageTypeTask:
	case groove.MessageTypeError:
	default:
		fmt.Println("Unknown message type", m.Type)
	}

	return nil, errors.New("unknown message type")
}

type Subscription struct {
	groove.Subscription
	Connection *Connection
}

func (g *GrooveMaster) disconnected(conn *Connection) {
	g.wsMX.Lock()

	// Remove connection
	for i, c := range g.connections {
		if c == conn {
			g.connections = append(g.connections[:i], g.connections[i+1:]...)

			for _, _ = range c.activeTasks {
				// TODO nack tasks
			}

			break
		}
	}

	// Remove subscriptions
	for i, s := range g.subscriptions {
		if s.Connection == conn {
			g.subscriptions = append(g.subscriptions[:i], g.subscriptions[i+1:]...)
		}
	}
	g.wsMX.Unlock()
}

func (g *GrooveMaster) handleWS(res http.ResponseWriter, req *http.Request) {
	connection := NewConnection(g)

	conn, err := wsupgrader.Upgrade(res, req, nil)
	if err != nil {
		fmt.Println("Failed to upgrade websocket", err)
		return
	}

	conMX := sync.Mutex{}

	// Message writer will write any messages on the connections "writer" chan to the connection
	go func() {
		for m := range connection.writer {
			mb, err := msgpack.Marshal(m)
			if err != nil {
				fmt.Println("Failed to marshal message", err)
				continue
			}

			conMX.Lock()
			err = conn.WriteMessage(websocket.BinaryMessage, mb)
			if err != nil {
				fmt.Println("Failed to write message", err)
			}
			conMX.Unlock()
		}
	}()

	// Write control packages to keep the connection alive
	go func() {
		for {
			time.Sleep(5 * time.Second)

			conMX.Lock()
			err = conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second))
			if err != nil {
				if err == websocket.ErrCloseSent {
					conMX.Unlock()
					break
				} else if strings.Contains(err.Error(), "use of closed network connection") {
					conMX.Unlock()
					break
				} else if strings.Contains(err.Error(), "an existing connection was forcibly closed") {
					conMX.Unlock()
					break
				} else {
					fmt.Println("Error writing keepalive", err)
				}
			}
			conMX.Unlock()
		}
	}()

	// Goroutine that reads incoming messages from the connection and puts them in the reader channel
	go func() {
		for {
			t, m, err := conn.ReadMessage()
			if err != nil {
				switch err.(type) {
				case *websocket.CloseError:
					closeErr := err.(*websocket.CloseError)

					switch closeErr.Code {
					case websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived:
						// Don't do anything
					default:
						fmt.Println("Error reading websocket message", err)
					}

					_ = conn.Close()
				case *net.OpError:
					fmt.Println("websocket net error ", err)
				default:
					fmt.Println("websocket unknown error ", err)
				}

				break
			}

			// Check to make sure the message is a binary message
			if t != websocket.BinaryMessage {
				fmt.Println("Incorrect message type", t)
			}

			var msg groove.Message

			err = msgpack.Unmarshal(m, &msg)
			if err != nil {
				fmt.Println("Failed to unmarshal ws message", err)
			} else {
				connection.reader <- msg
			}
		}

		// Once this point is reached, the connection has been closed
		g.disconnected(connection)
	}()

	g.wsMX.Lock()
	g.connections = append(g.connections, connection)
	g.wsMX.Unlock()
}
