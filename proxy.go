package mongoproxy

import (
	"io"
	"log"
	"net"
)

const (
	defaultListenPort = "28017"
	defaultListenAddr = "127.0.0.1"

	defaultTargetPort = "27017"
	defaultTargetAddr = "127.0.0.1"
)

func ListenAndServe(opts ...Option) {
	cfg := Config{
		ListenAddr: defaultListenAddr + ":" + defaultListenPort,
		TargetAddr: defaultTargetAddr + ":" + defaultTargetPort,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cfg.ListenAddr, err)
	}

	log.Printf("Proxy server listening on %s, forwarding to %s", cfg.ListenAddr, cfg.TargetAddr)

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			continue
		}

		go handleConnection(clientConn, cfg.TargetAddr)
	}
}

func handleConnection(clientConn net.Conn, targetAddr string) {
	defer clientConn.Close()

	serverConn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Printf("failed to connect to target %s: %v", targetAddr, err)
	}

	defer serverConn.Close()

	// Start forwarding in both directions.
	go proxyClientToMongo(clientConn, serverConn)
	proxyMongoToClient(serverConn, clientConn)
}

func proxyClientToMongo(src net.Conn, dst net.Conn) {
	if _, err := io.Copy(dst, src); err != nil {
		log.Printf("error proxying from client to MongoDB: %v", err)
	}
	dst.Close()
}

func proxyMongoToClient(src net.Conn, dst net.Conn) {
	if _, err := io.Copy(dst, src); err != nil {
		log.Printf("error proxying from MongoDB to client: %v", err)
	}
	dst.Close()
}
