package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"flag"

	"fmt"

	"github.com/berkaroad/packetio"
)

var DefaultServerAddr = "127.0.0.1:4000"
var consoleLog = log.New(os.Stdout, "[packetio] ", log.LstdFlags)

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
		consoleLog.Println("Start")
		data, _ := p.ReadPacket()
		consoleLog.Println(string(data))
		p.WritePacket([]byte{'p'})
		for j := 0; j < 10; j++ {
			for i := 0; i < 100000; i++ {
				if err := p.WritePacket([]byte("Hello, i'm jerry " + strconv.Itoa(i*(j+1)))); err != nil {
					consoleLog.Println(err)
					break
				}
			}
			consoleLog.Println("send", j)
		}
	}
}
