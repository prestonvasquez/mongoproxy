package mongoproxy

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/wiremessage"
)

const (
	defaultListenPort = "28017"
	defaultListenAddr = "127.0.0.1"

	defaultTargetPort = "27017"
	defaultTargetAddr = "127.0.0.1"
)

type connInfo struct {
	cs   *connstring.ConnString
	addr string // resolved target address
}

// ListenAndServe starts the proxy on listenAddr (or default)
// forwarding to targetAddr (or default).
func ListenAndServe(opts ...Option) {
	cfg := Config{
		ListenAddr: defaultListenAddr + ":" + defaultListenPort,
		TargetAddr: defaultTargetAddr + ":" + defaultTargetPort,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	targetURI := cfg.TargetURI
	if targetURI == "" {
		targetURI = "mongodb://" + cfg.TargetAddr
	}

	targetCS, err := connstring.Parse(targetURI)
	if err != nil {
		log.Fatalf("failed to parse target URI %q: %v", targetURI, err)
	}

	targetAddr, err := resolveTarget(targetCS)
	if err != nil {
		log.Printf("failed to resolve target address: %v", err)
	}

	targetConnInfo := connInfo{
		cs:   targetCS,
		addr: targetAddr,
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cfg.ListenAddr, err)
	}

	log.Printf("Proxy server listening on %s → %s", cfg.ListenAddr, targetAddr)

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConnection(clientConn, targetConnInfo)
	}
}

//func dialTLS(ci connInfo) (net.Conn, error) {
//	cs := ci.cs
//	tlsCfg := &tls.Config{
//		ServerName:         cs.Hosts[0],    // Use the first host for SNI
//		InsecureSkipVerify: cs.SSLInsecure, // Skip TLS verification for simplicity
//	}
//
//	// Load CA if specified
//	if cs.SSLCaFileSet {
//		roots := x509.NewCertPool()
//
//		pem, err := os.ReadFile(cs.SSLCaFile)
//		if err != nil {
//			return nil, fmt.Errorf("failed to read CA file %s: %w", cs.SSLCaFile, err)
//		}
//
//		if !roots.AppendCertsFromPEM(pem) {
//			return nil, fmt.Errorf("failed to append CA certs from %s", cs.SSLCaFile)
//		}
//
//		tlsCfg.RootCAs = roots
//	}
//
//	// Load client certificate/key pair
//	if cs.SSLCertificateFileSet && cs.SSLPrivateKeyFileSet {
//		cert, err := tls.LoadX509KeyPair(cs.SSLCertificateFile, cs.SSLPrivateKeyFile)
//		if err != nil {
//			return nil, fmt.Errorf("failed to load client cert/key pair: %w", err)
//		}
//
//		tlsCfg.Certificates = []tls.Certificate{cert}
//	} else if cs.SSLClientCertificateKeyFileSet {
//		cert, err := tls.LoadX509KeyPair(cs.SSLClientCertificateKeyFile, cs.SSLClientCertificateKeyFile)
//		if err != nil {
//			return nil, fmt.Errorf("failed to load client cert/key pair: %w", err)
//		}
//
//		tlsCfg.Certificates = []tls.Certificate{cert}
//	}
//
//	return tls.Dial("tcp", ci.addr, tlsCfg)
//}

func handleConnection(clientConn net.Conn, targetConnInfo connInfo) {
	defer clientConn.Close()

	var serverConn net.Conn
	var err error

	targetCS := targetConnInfo.cs

	// Let the driver build a clientoptions for us
	clientOpts := options.Client().ApplyURI(targetConnInfo.cs.Original)

	// Decide TLS v plain TCP from cs.SSL
	if targetCS.SSLSet && targetCS.SSL {
		serverConn, err = tls.Dial("tcp", targetConnInfo.addr, clientOpts.TLSConfig)
		log.Printf("dialing target %s with TLS", targetConnInfo.addr)
	} else {
		serverConn, err = net.Dial("tcp", targetConnInfo.addr)
	}

	if err != nil {
		log.Printf("failed to dial target %s: %v", targetConnInfo.addr, err)
		return // <— bail if we can’t reach the real server
	}
	defer serverConn.Close()

	// proxy both directions
	go proxyClientToMongo(clientConn, serverConn)
	proxyMongoToClient(serverConn, clientConn)
}

// pendingMap tracks per-client pending test instructions.
var pendingMap = &connMap{m: make(map[net.Conn]*testInstruction)}

type connMap struct {
	mu sync.Mutex
	m  map[net.Conn]*testInstruction
}

func (c *connMap) Set(conn net.Conn, instr *testInstruction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[conn] = instr
}

func (c *connMap) Take(conn net.Conn) *testInstruction {
	c.mu.Lock()
	defer c.mu.Unlock()
	instr := c.m[conn]
	delete(c.m, conn)
	return instr
}

// proxyClientToMongo intercepts OP_MSG, strips proxyTest, and forwards cleaned message.
func proxyClientToMongo(src net.Conn, dst net.Conn) {
	for {
		raw, err := readWireMessage(src)
		if err != nil {
			return
		}
		// parse header
		_, _, _, opcode, body, ok := wiremessage.ReadHeader(raw)
		if !ok || opcode != wiremessage.OpMsg {
			dst.Write(raw)
			continue
		}
		// skip flags
		_, body, ok = wiremessage.ReadMsgFlags(body)
		if !ok {
			dst.Write(raw)
			continue
		}
		// skip section type
		stype, body, ok := wiremessage.ReadMsgSectionType(body)
		if !ok || stype != wiremessage.SingleDocument {
			dst.Write(raw)
			continue
		}
		// read the first document
		doc, _, ok := wiremessage.ReadMsgSectionSingleDocument(body)
		if !ok {
			dst.Write(raw)
			continue
		}
		// strip proxyTest
		cleanDoc, instr, err := parseProxy(bson.Raw(doc))
		if err != nil {
			log.Printf("parseProxy error: %v", err)
			return
		}
		if instr != nil {

			pendingMap.Set(src, instr)
		}
		// reconstruct
		docStart := 16 + 4 + 1
		origLen := len(doc)
		newLen := len(raw) - origLen + len(cleanDoc)
		// length prefix
		out := make([]byte, 0, newLen)
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(newLen))
		out = append(out, lenBuf...)
		// header+flags+sectionType
		out = append(out, raw[4:docStart]...)
		// cleaned doc
		out = append(out, cleanDoc...)
		// rest
		out = append(out, raw[docStart+origLen:]...)
		// send
		dst.Write(out)
	}
}

// readWireMessage reads a length-prefixed MongoDB wire message from src.
func readWireMessage(src io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(src, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int(binary.LittleEndian.Uint32(lenBuf[:]))

	msg := make([]byte, length)
	copy(msg, lenBuf[:])
	if _, err := io.ReadFull(src, msg[4:]); err != nil {
		return nil, err
	}
	return msg, nil
}

// applyActions processes a sequence of actions on the response buffer.
func applyActions(buf []byte, dst net.Conn, actions []action) {
	offset := 0
	sendAction := false
	for _, act := range actions {
		if act.DelayMs != nil {
			log.Printf("Delaying %d ms before sending next action", act.DelayMs)
			time.Sleep(time.Duration(*act.DelayMs) * time.Millisecond)
		}
		if act.SendBytes != nil {
			log.Printf("Sending %d bytes from offset %d", *act.SendBytes, offset)
			end := offset + *act.SendBytes
			if end > len(buf) {
				end = len(buf)
			}
			dst.Write(buf[offset:end])
			offset = end
			sendAction = true
		}
		if act.SendAll != nil {
			log.Printf("Sending remaining bytes from offset %d", offset)
			dst.Write(buf[offset:])
			offset = len(buf)
			sendAction = true
		}
	}
	if offset < len(buf) && !sendAction {
		dst.Write(buf[offset:])
	}
}

// proxyMongoToClient waits for an instruction on a matching connection, applies it to that first reply, then continues.
func proxyMongoToClient(src net.Conn, dst net.Conn) {
	for {
		raw, err := readWireMessage(src)
		if err != nil {
			return
		}

		instr := pendingMap.Take(dst)
		if instr == nil {
			// Not our target reply yet
			dst.Write(raw)
			continue
		}

		// Apply actions to the raw reply
		applyActions(raw, dst, instr.Actions)
		break
	}

	// Forward remaining data
	if _, err := io.Copy(dst, src); err != nil {
		log.Printf("error proxying mongo→client: %v", err)
	}

	// Half-close write side
	if tcp, ok := dst.(*net.TCPConn); ok {
		tcp.CloseWrite()
	} else {
		dst.Close()
	}
}
