package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"github.com/berkaroad/larvamq/larvamqd/conn"
)

const DefaultPort int = 4000

var consoleLog = log.New(os.Stdout, "[larvamqd] ", log.LstdFlags)

var productorMgr = conn.NewClientManager()
var consumerMgr = conn.NewClientManager()
var broadcastMsgChan = make(chan *conn.Message, 100000)
var failMsgChan = make(chan *conn.Message, 30000)

func main() {
	port := 0
	flag.IntVar(&port, "p", DefaultPort, "please input listen port")
	flag.Parse()

	if listener, err := net.Listen("tcp", ":"+strconv.Itoa(port)); err != nil {
		consoleLog.Println(err)
	} else {
		go func() {
			http.ListenAndServe("127.0.0.1:6060", nil)
		}()
		go func() {
			for {
				if consumerMgr.Len() == 0 {
					time.Sleep(10 * time.Second)
					continue
				}
				consumerMgr.ConsumeWithLoadBalance(broadcastMsgChan, failMsgChan)
			}
		}()
		for {
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

	for {
		data, err := client.Receive()
		if err != nil {
			data = nil
			if clientType == conn.CLIENT_TYPE_PRODUCTOR {
				productorMgr.RemoveClient(clientID)
			} else if clientType == conn.CLIENT_TYPE_CONSUMER {
				consumerMgr.RemoveClient(clientID)
			}
			client.Close()
			consoleLog.Println(err)
			break
		} else {
			if clientType == conn.CLIENT_TYPE_PRODUCTOR {
				broadcastMsgChan <- &conn.Message{ClientID: clientID, Msg: data}
				client.Send([]byte("ACK"))
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
