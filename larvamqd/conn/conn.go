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

const (
	CLIENT_TYPE_PRODUCTER ClientType = 1
	CLIENT_TYPE_CONSUMER  ClientType = 2
)

type ClientType int32

type Client struct {
	id     uuid.UUID
	t      int32
	closed int32
	c      net.Conn
	p      *packetio.PacketIO
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

func (c *Client) SetClientType(t ClientType) {
	atomic.CompareAndSwapInt32(&c.t, 0, int32(t))
}

func (c *Client) ClientType() ClientType {
	return ClientType(atomic.LoadInt32(&c.t))
}

func (c *Client) SayHello() {
	c.Send([]byte("Welcome to use LarvaMQ"))
}

func (c *Client) Send(data []byte) error {
	if !c.IsClosed() {
		return c.p.WritePacket(data)
	}
	return errors.New("is closed")
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
	l          *sync.Mutex
	clientList *list.List
}

func NewClientManager() *ClientManager {
	cm := &ClientManager{}
	cm.l = &sync.Mutex{}
	cm.clientList = list.New()
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

func (cm *ClientManager) RemoveAllClient() {
	defer cm.l.Unlock()
	cm.l.Lock()
	cm.clientList = cm.clientList.Init()
}

func (cm *ClientManager) Len() int {
	return cm.clientList.Len()
}

func (cm *ClientManager) SendWithLB(messageChan chan *Message, failMessageChan chan *Message) {
	wg := new(sync.WaitGroup)
	clientChan := make(chan *Client, cm.Len())
	for e := cm.clientList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		go func(clientChan chan *Client) {
			client := <-clientChan
			select {
			case data := <-failMessageChan:
				if err := client.Send(data.Msg); err != nil {
					cm.RemoveClient(client.ID())
					client.Close()
					failMessageChan <- data
				}
			case data := <-messageChan:
				if err := client.Send(data.Msg); err != nil {
					cm.RemoveClient(client.ID())
					client.Close()
					failMessageChan <- data
				}
			}
			wg.Done()
		}(clientChan)
		clientChan <- e.Value.(*Client)
	}
	wg.Wait()
}

type Message struct {
	ClientID uuid.UUID
	Msg      []byte
}
