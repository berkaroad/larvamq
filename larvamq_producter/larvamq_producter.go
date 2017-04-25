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
var consoleLog = log.New(os.Stdout, "[larvamq_producter] ", log.LstdFlags)

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
		var totalSuccessCounter int64
		timer := time.NewTimer(time.Second)
		go func() {
			for {
				<-timer.C
				consoleLog.Println("sent:", atomic.SwapInt32(&counter, 0), " fail:", atomic.SwapInt32(&failCounter, 0), " total success:", atomic.LoadInt64(&totalSuccessCounter))
				timer.Reset(time.Second)
			}
		}()
		time.Sleep(time.Second * 2)
		// 		text := []byte(`
		// golang语言中map的初始化及使用 | Go语言中文网 | Golang中文社区...
		// 2015年2月7日 - ("Key Not Found") } // 遍历map for k, v := range m1 { fmt....Go语言中文网,中国 Golang 社区,致力于构建完善的 Golang 中文社区,Go语言爱好...
		// studygolang.com/articl...  - 百度快照 - 评价
		// Go语言小知识之map遍历 - zxh的专栏 - 博客频道 - CSDN.NET
		// 2017年4月13日 - Go语言里的map,是不保证遍历顺序的(这一点很好理解)。甚至同样内容的map,两次...23229975/is-it-safe-to-remove-selected-keys-from-golang-map-withi...
		// blog.csdn.net/zxhoo/ar...  - 百度快照 - 1748条评价
		// golang map 用原生range遍历不能保证顺序输出 - hificamera的博客...
		// 2016年6月13日 - 按照之前我对map的理解,map中的数据应该是有序二叉树的存储顺序,正常的遍历也应该是有序的遍历和输出,但实际试了一下,却发现并非如此,网上查了下,发现...
		// blog.csdn.net/hificame...  - 百度快照 - 1748条评价
		// abcdefghijklmn
		// 		`)
		text := []byte("I'm jerry")
		for {
			if err := p.WritePacket(text); err != nil {
				consoleLog.Println(err)
				break
			}
			atomic.AddInt32(&counter, 1)
			go func() {
				if data, err := p.ReadPacket(); err != nil {
					consoleLog.Println(err)
				} else {
					if string(data) == "ACK" {
						atomic.AddInt64(&totalSuccessCounter, 1)
					} else {
						atomic.AddInt32(&failCounter, 1)
						time.Sleep(time.Millisecond * 100)
					}
				}
			}()
		}
	}
}
