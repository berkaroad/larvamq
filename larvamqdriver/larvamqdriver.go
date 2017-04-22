package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"github.com/berkaroad/packetio"
)

var port = 4000
var consoleLog = log.New(os.Stdout, "[packetio] ", log.LstdFlags)

func init() {
	packetio.DEBUG = false
}

func main() {
	conn, _ := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	p := packetio.New(conn)
	consoleLog.Println("Start")
	data, _ := p.ReadPacket()
	consoleLog.Println(string(data))
	// seq := make(chan int)
	// wg := &sync.WaitGroup{}
	for j := 0; j < 10; j++ {
		// go func(seq chan int) {
		// j := <-seq
		for i := 0; i < 100000; i++ {
			if err := p.WritePacket([]byte("Hello, i'm jerry " + strconv.Itoa(i*(j+1)))); err != nil {
				consoleLog.Println(err)
				break
			}
		}
		consoleLog.Println("send", j)
		// wg.Done()
		// }(seq)
		// seq <- j
		// wg.Add(1)
	}
	// wg.Wait()
}
