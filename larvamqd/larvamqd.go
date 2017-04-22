package main

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"time"

	"github.com/berkaroad/larvamq/larvamqd/conn"
	"github.com/berkaroad/uuid"
)

var port = 4000
var consumerMgr = conn.NewClientManager()
var cacheChan = make(chan *BroadcastMessage, 100000)

type BroadcastMessage struct {
	ClientID uuid.UUID
	Msg      []byte
}

func main() {
	go func() {
		http.ListenAndServe("127.0.0.1:6060", nil)
	}()
	listener, _ := net.Listen("tcp", ":"+strconv.Itoa(port))
	go func() {
		for true {
			println(len(cacheChan))
			if consumerMgr.Len() == 0 {
				time.Sleep(time.Second)
				continue
			}
			broadcastMsg := <-cacheChan
			consumerMgr.Broadcast(broadcastMsg.Msg)
			broadcastMsg = nil
		}
	}()
	for true {
		c, _ := listener.Accept()
		client := conn.NewClient(c)
		go handleClient(client)
	}
}

func handleClient(client *conn.Client) {
	client.SayHello()
	clientID := client.ID()
	if _, err := client.Receive(); err != nil {
		consumerMgr.AddClient(client)
	}
	for true {
		data, err := client.Receive()
		if err != nil {
			data = nil
			consumerMgr.RemoveClient(client.ID())
			client.Close()
			fmt.Println("close", err)
			break
		} else {
			cacheChan <- &BroadcastMessage{ClientID: clientID, Msg: data}
			data = nil
		}
	}
}
