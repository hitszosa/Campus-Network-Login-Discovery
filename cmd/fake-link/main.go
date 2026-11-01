package main

import (
	"flag"
	"log"
	"net"
	"os"
)

func pipe(dst, src net.Conn, dropFile string) {
	defer dst.Close()
	defer src.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := src.Read(buffer)
		if err != nil {
			return
		}
		if _, err := os.Stat(dropFile); err == nil {
			continue
		}
		// Write 可能只发送部分字节，所以一直写到完成。
		data := buffer[:n]
		for len(data) > 0 {
			n, err := dst.Write(data)
			if err != nil {
				return
			}
			data = data[n:]
		}
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:1884", "listen address")
	upstream := flag.String("upstream", "127.0.0.1:1883", "Broker address")
	dropFile := flag.String("drop-file", "/tmp/cnld-blackhole", "drop traffic while file exists")
	flag.Parse()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	for {
		agent, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		broker, err := net.Dial("tcp", *upstream)
		if err != nil {
			agent.Close()
			continue
		}
		go pipe(agent, broker, *dropFile)
		go pipe(broker, agent, *dropFile)
	}
}
