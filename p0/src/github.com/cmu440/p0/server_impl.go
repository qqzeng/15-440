// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
)

const (
	MSG_BUFFERED_SIZE  = 100
	CHAN_SIZE          = 1
	CONN_BUFFERED_SIZE = 1
	SERVER_HOST        = "localhost"
)

type multiEchoServer struct {
	// TODO: implement this!
	clientNum     int           // number of clients.
	chanConnList  chan net.Conn // channel to notice a coming connection.
	chanStop      chan bool     // channel to indicate stop server.
	chanRespMap   map[net.Conn]compConn
	chanResp      chan string
	chanConnClose chan net.Conn
	ln            net.Listener
}

type compConn struct {
	conn           net.Conn
	chanRespBuffer chan string
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	// TODO: implement this!
	mes := &multiEchoServer{
		clientNum:     0,
		chanStop:      make(chan bool, CHAN_SIZE),
		chanConnList:  make(chan net.Conn, CONN_BUFFERED_SIZE),
		chanRespMap:   make(map[net.Conn]compConn, CONN_BUFFERED_SIZE),
		chanResp:      make(chan string, CONN_BUFFERED_SIZE),
		chanConnClose: make(chan net.Conn, CONN_BUFFERED_SIZE),
	}
	return MultiEchoServer(mes)
}

func (mes *multiEchoServer) Start(port int) error {
	// TODO: implement this!
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Printf("%v\n", err)
		return err
	}
	// mes.ln = ln
	go mes.handleStuff()
	go mes.handleResp()
	go func() {
		for {
			fmt.Println("Waiting for a connection.")
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("Error on accept: ", err)
				continue
			}
			mes.chanConnList <- conn

			go mes.handleConn(conn)
		}
	}()
	return nil
}

func (mes *multiEchoServer) Close() {
	// TODO: implement this!
	fmt.Printf("server Close called..\n")
	mes.chanStop <- true
}

func (mes *multiEchoServer) Count() int {
	// TODO: implement this!
	return mes.clientNum
}

// TODO: add additional methods/functions below!

func (mes *multiEchoServer) handleConn(conn net.Conn) {
	fmt.Println("Reading from connection..")

	rb := bufio.NewReader(conn)
	for {
		msg, e := rb.ReadString('\n')
		if e != nil {
			break
		}
		// fmt.Printf("Read: %v\n", msg)
		mes.chanResp <- msg
	}
	mes.chanConnClose <- conn
}

func (mes *multiEchoServer) handleResp() {
	for {
		msg := <-mes.chanResp
		for con, cc := range mes.chanRespMap {
			select {
			case cc.chanRespBuffer <- msg:
			default:
				fmt.Printf("discard message %v of connection %v, current pending message size = %v.\n", msg, con, len(cc.chanRespBuffer))
			}
		}
	}
}

func (mes *multiEchoServer) echoResp(cc compConn) {
	for {
		con := cc.conn
		msg := <-cc.chanRespBuffer
		_, err := con.Write([]byte(string(msg)))
		if err != nil {
			return
		}
	}
}

func (mes *multiEchoServer) handleStuff() {
	for {
		select {
		case <-mes.chanStop:
			for _, cc := range mes.chanRespMap {
				mes.chanConnClose <- cc.conn
			}
			// mes.ln.Close()
			return
		case con := <-mes.chanConnList:
			if con != nil {
				mes.clientNum++
				mes.chanRespMap[con] = compConn{conn: con, chanRespBuffer: make(chan string, MSG_BUFFERED_SIZE)}
				go mes.echoResp(mes.chanRespMap[con])
			}
		case con := <-mes.chanConnClose:
			if con != nil {
				mes.clientNum--
				delete(mes.chanRespMap, con)
				con.Close()
			}
		}
	}
}
