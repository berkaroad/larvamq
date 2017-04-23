package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/berkaroad/packetio"
)

var DefaultServerAddr = "127.0.0.1:4000"
var consoleLog = log.New(os.Stdout, "[larvamq_productor] ", log.LstdFlags)

func init() {
	packetio.DEBUG = false
}

func main() {
	serverAddr := ""
	flag.StringVar(&serverAddr, "s", DefaultServerAddr, "server address to connect larvamqd")
	topicName := ""
	flag.StringVar(&topicName, "t", "public", "topic name")
	flag.Parse()
	net.ResolveTCPAddr("tcp", serverAddr)

	if conn, err := net.Dial("tcp", serverAddr); err != nil {
		fmt.Println(err)
	} else {
		p := packetio.New(conn)
		data, _ := p.ReadPacket()
		consoleLog.Println(string(data))
		p.WritePacket([]byte("p:" + topicName))

		var counter int32
		var failCounter int32
		timer := time.NewTimer(time.Second)
		go func() {
			for {
				<-timer.C
				consoleLog.Println("sent:", atomic.SwapInt32(&counter, 0), " fail:", atomic.SwapInt32(&failCounter, 0))
				timer.Reset(time.Second)
			}
		}()
		time.Sleep(time.Second * 2)
		go func() {
			for {
				if data, err := p.ReadPacket(); err != nil {
					consoleLog.Println(err)
					break
				} else {
					if string(data) != "ACK" {
						atomic.AddInt32(&failCounter, 1)
						println(string(data))
					}
				}
			}
		}()
		for {
			if err := p.WritePacket([]byte("Hello, i'm jerry ")); err != nil {
				consoleLog.Println(err)
				break
			} else {
				atomic.AddInt32(&counter, 1)
			}
		}
	}
}
