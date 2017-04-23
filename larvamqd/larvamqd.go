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

	"github.com/berkaroad/larvamq/larvamqd/conn"
)

const DefaultPort int = 4000

var consoleLog = log.New(os.Stdout, "[larvamqd] ", log.LstdFlags)

var topicMgr = conn.NewTopicManager()

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
		for {
			c, _ := listener.Accept()
			client := conn.NewClient(c)
			go handleClient(client)
		}
	}
}

func handleClient(client *conn.Client) {
	if topicName, err := shakeHand(client); err != nil {
		consoleLog.Println(err)
	} else {
		clientID := client.ID()
		clientType := client.ClientType()
		topic := topicMgr.CreateOrJoinTopic(topicName, client)
		println(topicName)
		for {
			data, err := client.Receive()
			if err != nil {
				data = nil
				topic.RemoveClient(clientID, clientType)
				client.Close()
				consoleLog.Println(err)
				break
			} else {
				if clientType == conn.CLIENT_TYPE_PRODUCTOR {
					topic.MsgChan <- &conn.Message{ClientID: clientID, Msg: data}
					client.Send([]byte("ACK"))
				}
				data = nil
			}
		}
	}
}

func shakeHand(client *conn.Client) (string, error) {
	client.SayHello()
	data, err := client.Receive()
	if err != nil || len(data) < 3 {
		client.Close()
		return "", errors.New("shake hand error")
	} else if string(data[0:2]) == "p:" {
		client.SetClientType(conn.CLIENT_TYPE_PRODUCTOR)
	} else if string(data[0:2]) == "c:" {
		client.SetClientType(conn.CLIENT_TYPE_CONSUMER)
	} else {
		client.Close()
		return "", errors.New("shake hand error")
	}
	return string(data[2:]), nil
}
