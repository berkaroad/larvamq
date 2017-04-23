package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"time"

	"flag"

	"github.com/berkaroad/larvamq/larvamqd/conn"
	"github.com/berkaroad/uuid"
)

const DefaultPort int = 4000

var productorMgr = conn.NewClientManager()
var consumerMgr = conn.NewClientManager()
var cacheChan = make(chan *BroadcastMessage, 100000)

type BroadcastMessage struct {
	ClientID uuid.UUID
	Msg      []byte
}

func main() {
	port := 0
	flag.IntVar(&port, "p", DefaultPort, "please input listen port")
	flag.Parse()

	if listener, err := net.Listen("tcp", ":"+strconv.Itoa(port)); err != nil {
		fmt.Println(err)
	} else {
		go func() {
			http.ListenAndServe("127.0.0.1:6060", nil)
		}()
		go func() {
			for true {
				// println(len(cacheChan))
				if consumerMgr.Len() == 0 {
					time.Sleep(10 * time.Second)
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
}

func handleClient(client *conn.Client) {
	if err := shakeHand(client); err != nil {
		return
	}
	clientID := client.ID()
	clientType := client.ClientType()
	for true {
		data, err := client.Receive()
		if err != nil {
			data = nil
			if clientType == conn.CLIENT_TYPE_PRODUCTOR {
				productorMgr.RemoveClient(clientID)
			} else if clientType == conn.CLIENT_TYPE_CONSUMER {
				consumerMgr.RemoveClient(clientID)
			}
			client.Close()
			fmt.Println("close", err)
			break
		} else {
			if clientType == conn.CLIENT_TYPE_PRODUCTOR {
				cacheChan <- &BroadcastMessage{ClientID: clientID, Msg: data}
			}
			data = nil
		}
	}
}

func shakeHand(client *conn.Client) error {
	client.SayHello()
	if data, err := client.Receive(); err != nil {
		client.Close()
		return errors.New("shake hand error")
	} else if data[0] == 'p' {
		client.SetClientType(conn.CLIENT_TYPE_PRODUCTOR)
		productorMgr.AddClient(client)
	} else if data[0] == 'c' {
		client.SetClientType(conn.CLIENT_TYPE_CONSUMER)
		consumerMgr.AddClient(client)
	} else {
		client.Close()
		return errors.New("shake hand error")
	}
	return nil
}
