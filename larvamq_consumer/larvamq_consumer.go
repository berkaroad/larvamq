package main

import (
	"log"
	"net"
	"os"

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
		p.WritePacket([]byte{'c'})
		i := 0
		for true {
			if _, err := p.ReadPacket(); err != nil {
				consoleLog.Println(err)
				break
			} else {
				i++
				if i%10000 == 0 {
					consoleLog.Println("received")
				}
			}
		}
	}
}
