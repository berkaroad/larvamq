package conn

import (
	"sync"
	"time"

	"github.com/berkaroad/uuid"
)

type Topic struct {
	name         string
	MsgChan      chan *Message
	FailMsgChan  chan *Message
	ExitChan     chan int
	productorMgr *ClientManager
	consumerMgr  *ClientManager
}

func NewTopic(name string, client *Client) *Topic {
	t := new(Topic)
	t.name = name
	t.MsgChan = make(chan *Message, 1000)
	t.FailMsgChan = make(chan *Message, 1000)
	t.ExitChan = make(chan int)
	t.productorMgr = NewClientManager()
	t.consumerMgr = NewClientManager()

	switch client.ClientType() {
	case CLIENT_TYPE_PRODUCTOR:
		t.productorMgr.AddClient(client)
	case CLIENT_TYPE_CONSUMER:
		t.consumerMgr.AddClient(client)
	default:
		return nil
	}
	return t
}

func (t *Topic) Name() string {
	return t.name
}

func (t *Topic) AddClient(client *Client) {
	switch client.ClientType() {
	case CLIENT_TYPE_PRODUCTOR:
		t.productorMgr.AddClient(client)
	case CLIENT_TYPE_CONSUMER:
		t.consumerMgr.AddClient(client)
	}
}

func (t *Topic) RemoveClient(clientID uuid.UUID, clientType ClientType) {
	switch clientType {
	case CLIENT_TYPE_PRODUCTOR:
		t.productorMgr.RemoveClient(clientID)
	case CLIENT_TYPE_CONSUMER:
		t.consumerMgr.RemoveClient(clientID)
	}
}

func (t *Topic) SendWithLB() {
	if t.consumerMgr.Len() == 0 {
		time.Sleep(3 * time.Second)
	} else {
		t.consumerMgr.SendWithLB(t.MsgChan, t.FailMsgChan)
	}
}

type TopicManager struct {
	l         *sync.Mutex
	topicList map[string]*Topic
}

func NewTopicManager() *TopicManager {
	tm := new(TopicManager)
	tm.l = new(sync.Mutex)
	tm.topicList = make(map[string]*Topic)
	return tm
}

func (tm *TopicManager) CreateOrJoinTopic(topicName string, client *Client) *Topic {
	defer tm.l.Unlock()
	tm.l.Lock()

	topic, ok := tm.topicList[topicName]
	if ok {
		topic.AddClient(client)
	} else {
		topic = NewTopic(topicName, client)
		tm.topicList[topicName] = topic
		go func() {
			for {
				select {
				case <-topic.ExitChan:
					break
				default:
					topic.SendWithLB()
				}
			}
		}()
	}
	return topic
}

func (tm *TopicManager) RemoveTopic(topicName string) {
	defer tm.l.Unlock()
	tm.l.Lock()
	defer tm.l.Unlock()
	tm.l.Lock()

	topic, ok := tm.topicList[topicName]
	if ok {
		topic.ExitChan <- 1
		delete(tm.topicList, topicName)
	}
}

func (tm *TopicManager) Len() int {
	defer tm.l.Unlock()
	tm.l.Lock()

	return len(tm.topicList)
}
