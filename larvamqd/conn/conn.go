package conn

import (
	"container/list"
	"errors"
	"net"

	"sync"
	"sync/atomic"

	"github.com/berkaroad/packetio"
	"github.com/berkaroad/uuid"
)

func init() {
	packetio.DEBUG = false
}

type Client struct {
	id     uuid.UUID
	c      net.Conn
	p      *packetio.PacketIO
	closed int32
}

func NewClient(c net.Conn) *Client {
	client := new(Client)
	client.id = uuid.New()
	client.c = c
	client.p = packetio.New(c)
	return client
}

func (c *Client) ID() uuid.UUID {
	return c.id
}

func (c *Client) SayHello() {
	c.SendText("Welcome to use LarvaMQ")
}

func (c *Client) Send(data []byte) error {
	if !c.IsClosed() {
		return c.p.WritePacket(data)
	}
	return errors.New("is closed")
}

func (c *Client) SendText(text string) error {
	return c.Send(append([]byte(text), '\n'))
}

func (c *Client) Receive() ([]byte, error) {
	if !c.IsClosed() {
		return c.p.ReadPacket()
	}
	return nil, errors.New("is closed")
}

func (c *Client) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

func (c *Client) Close() {
	if !c.IsClosed() {
		c.c.Close()
		c.c = nil
		c.p = nil
		atomic.StoreInt32(&c.closed, 1)
	}
}

type ClientManager struct {
	clientList *list.List
	l          *sync.Mutex
}

func NewClientManager() *ClientManager {
	cm := &ClientManager{}
	cm.clientList = list.New()
	cm.l = &sync.Mutex{}
	return cm
}

func (cm *ClientManager) AddClient(client *Client) {
	defer cm.l.Unlock()
	cm.l.Lock()
	cm.clientList.PushBack(client)
}

func (cm *ClientManager) RemoveClient(clientID uuid.UUID) {
	defer cm.l.Unlock()
	cm.l.Lock()
	for e := cm.clientList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Client).ID() == clientID {
			cm.clientList.Remove(e)
			break
		}
	}
}

func (cm *ClientManager) Len() int {
	return cm.clientList.Len()
}

func (cm *ClientManager) Broadcast(data []byte) {
	wg := new(sync.WaitGroup)
	for e := cm.clientList.Front(); e != nil; e = e.Next() {
		client := e.Value.(*Client)
		wg.Add(1)
		go func() {
			client.Send(data)
			wg.Done()
		}()
	}
	wg.Wait()
}
