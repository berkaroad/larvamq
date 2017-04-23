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
var consoleLog = log.New(os.Stdout, "[larvamq_consumer] ", log.LstdFlags)

func init() {
	packetio.DEBUG = false
}

func main() {
	serverAddr := ""
	flag.StringVar(&serverAddr, "s", DefaultServerAddr, "server address to connect larvamqd")
	flag.Parse()
	net.ResolveTCPAddr("tcp", serverAddr)

	if conn, err := net.Dial("tcp", serverAddr); err != nil {
		fmt.Println(err)
	} else {
		p := packetio.New(conn)
		data, _ := p.ReadPacket()
		consoleLog.Println(string(data))
		p.WritePacket([]byte{'c'})

		var counter int32
		timer := time.NewTimer(time.Second)
		go func() {
			for {
				<-timer.C
				consoleLog.Println("received:", atomic.SwapInt32(&counter, 0))
				timer.Reset(time.Second)
			}
		}()
		time.Sleep(time.Second * 2)
		for {
			if _, err := p.ReadPacket(); err != nil {
				consoleLog.Println(err)
				break
			} else {
				atomic.AddInt32(&counter, 1)
			}
		}
	}
}
