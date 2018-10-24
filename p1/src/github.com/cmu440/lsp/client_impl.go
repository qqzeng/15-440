// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"io"
	"sync"
	"time"
)

type client struct {
	// TODO: implement this!
	hostport     string // e.g. 127.0.0.1:6666
	clientId     int
	conn         *lspnet.UDPConn // net connection with server
	params       *Params         // parameters of lsp
	connectionId int             // connectin id with server

	mu                  sync.Mutex
	chanReadFailure     chan bool     // catch client connection read failure, hence the connection has lost.
	chanConnClose       chan bool     // signal to close client connection, hence all background goroutines will exit.
	chanConBuild        chan bool     // start building connection with server.
	chanConBuildSuccess chan bool     // estibalish connection with server successfully.
	chanReply           chan *Message // signal to receive data messages from server.
	chanReplyAck        chan *Message // signal to receive data messages of Ack.
	chanMsgDone         chan bool     // all pending messages, which including those sent already but not received ack and those are being written.
	chanConnOnClose     chan bool     // signal to being to close connection. used before chanConnClose.
	chanMsgData         chan *Message // channel for messages reading from.
	chanWriteMsg        chan []byte   // channel for messages written temporally.
	chanSWIdle          chan bool     // check whether the sliding window is idle, so that ensure any coming messages should be blocked or not.
	chanWriteMsgCache   chan []byte   // channel caching all messages that must be sent before connection closing.
	remainEpoch         int           // epoch remained to retry operations.

	nextSendDataSeqNum    int // Sequence Number of Data Message to send next.
	receivedAckSeqNum     int // the highest sequence number of acked messages.
	nextReceiveDataSeqNum int // the next sequence number of data messages that expect to receive.

	latestSentDataMsg     *MessageWindowSent // maintain a sliding window for their w most recently sent data message for re-sending at a later time.
	latestReceivedDataMsg *MessageWindow     // maintain a sliding window for their w most recently sent acknowledgments for re-sending at a later time.
}

const (
	BUFFER_SIZE               = 1024 * 2
	CHAN_UNIT_SIZE            = 1
	CHAN_SIZE_MULTI           = 1024 * 10
	RETRY_READ_WRITE_INTERVAL = 20
)

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	if conn, err := connect(hostport); err != nil {
		return nil, err
	} else if conn != nil {
		fmt.Printf("Client: create client node.\n")
		cli := &client{
			hostport:            hostport,
			clientId:            getNextClientId(),
			conn:                conn,
			params:              params,
			chanReadFailure:     make(chan bool, 10),
			chanConnClose:       make(chan bool, 10),
			chanConBuild:        make(chan bool, CHAN_UNIT_SIZE),
			chanConBuildSuccess: make(chan bool, CHAN_UNIT_SIZE),
			chanReply:           make(chan *Message, CHAN_UNIT_SIZE),
			chanReplyAck:        make(chan *Message, CHAN_UNIT_SIZE),
			chanMsgData:         make(chan *Message, CHAN_SIZE_MULTI),
			chanWriteMsg:        make(chan []byte, CHAN_SIZE_MULTI),
			chanConnOnClose:     make(chan bool, CHAN_UNIT_SIZE),
			chanSWIdle:          make(chan bool, CHAN_SIZE_MULTI),
			chanMsgDone:         make(chan bool, CHAN_UNIT_SIZE),
			chanWriteMsgCache:   make(chan []byte, CHAN_SIZE_MULTI),

			remainEpoch: params.EpochLimit,

			nextSendDataSeqNum:    1,
			receivedAckSeqNum:     -1,
			nextReceiveDataSeqNum: 1,

			latestSentDataMsg:     NewMessageWindowSent(params.WindowSize),
			latestReceivedDataMsg: NewMessageWindow(params.WindowSize),
		}
		lspnet.EnableDebugLogs(true)
		go handleRequestConn(cli)
		cli.chanConBuild <- true
		select {
		case <-cli.chanConBuildSuccess:
			go cli.onRead()
			go handleStuff(cli)
			go doWrite(cli)
			return cli, nil
		}
		return nil, errors.New(fmt.Sprintf("Connection retry excedes maximmum(%v).", params.EpochLimit))
	}
	return nil, errors.New("baaroque error occurs when conneting...")
}

func saveMsgAndAck(cli *client, msg *Message, duplicate bool) {
	// put msg to sliding window.
	// messages in sliding window will be sorted by sequence number once a message is put into.
	cli.mu.Lock()
	if !duplicate {
		cli.latestReceivedDataMsg.Put(msg)
		fmt.Printf("client(%v): save received the new message(%v) for connection(%v).\n", cli.clientId, msg.String(), cli.ConnID())
	}
	// send ack although it may be duplicate.
	ackMsg := NewAck(cli.ConnID(), msg.SeqNum)
	cli.mu.Unlock()
	fmt.Printf("client(%v): save message(%v) and replay ack(%v).\n", cli.clientId, msg.String(), ackMsg.String())
	go onWrite(cli, ackMsg)
}

func saveSentMsg(cli *client, msg *Message) {
	fmt.Printf("client(%v) save sent msg(%v).\n", cli.clientId, msg.String())
	cli.mu.Lock()
	swmg := &SWMessage{
		ack: false,
		msg: msg,
	}
	cli.latestSentDataMsg.Put(swmg)
	cli.mu.Unlock()
	fmt.Printf("client(%v) save sent msg(%v) and returned.\n", cli.clientId, msg.String())
}

func updateLatestSentDataMsg(cli *client, msg *Message) {
	fmt.Printf("client(%v) begin to update latest sent data message by ack msg(%v).\n", cli.clientId, msg.String())
	cli.mu.Lock()
	ack := false
	for item := cli.latestSentDataMsg.First(); item != nil; item = cli.latestSentDataMsg.Next() {
		if item.msg.SeqNum == msg.SeqNum {
			if item.ack {
				fmt.Printf("client(%v) receive a duplicate ack(%v) message.\n", cli.clientId, item.msg.SeqNum)
			} else {
				fmt.Printf("client(%v) updated latest sent data message(%v) over.\n", cli.clientId, item.msg.String())
				item.ack = true
				ack = true
			}
			break
		}
	}
	tmp := cli.receivedAckSeqNum
	for item := cli.latestSentDataMsg.First(); item != nil; item = cli.latestSentDataMsg.Next() {
		if !item.ack {
			break
		}
		cli.receivedAckSeqNum = item.msg.SeqNum
	}
	fmt.Printf("client(%v) update its receivedAckSeqNum from %v to %v.\n", cli.clientId, tmp, cli.receivedAckSeqNum)
	cli.mu.Unlock()
	if ack {
		go checkPendingMsg("updateLatestSentDataMsg", cli)
		go checkSlidingWindowIdle(cli)
	}
}

func resetRemainEpoch(cli *client) {
	fmt.Printf("client(%v) resets its remained epoch to %v for server(%v).\n", cli.clientId, cli.params.EpochLimit, cli.hostport)
	cli.remainEpoch = cli.params.EpochLimit
}

func handleStuff(cli *client) {
	for cli.remainEpoch > 0 {
		select {
		case <-cli.chanConnClose:
			fmt.Printf("client(%v) exits cli.chanConnClose..\n", cli.clientId)
			return
		case readMsg := <-cli.chanReply:
			fmt.Printf("[1] client: receive Data message(%v).\n", readMsg.String())
			resetRemainEpoch(cli)
			// check the order of the message data.
			duplicate := cli.checkMsgSeqDuplicate(readMsg)
			if duplicate {
				fmt.Printf("client(%v): msg(%v) is duplicate, will not trigger time events.\n", cli.clientId, readMsg.String())
			}
			saveMsgAndAck(cli, readMsg, duplicate)
			// if !duplicate {
			// 	cli.chanMsgData <- readMsg
			// }
			doRead(cli)
		case readMsg := <-cli.chanReplyAck:
			fmt.Printf("[2] client: receive Ack message(%v).\n", readMsg.String())
			resetRemainEpoch(cli)
			updateLatestSentDataMsg(cli, readMsg)
			// cli.chanMsgData <- readMsg
		case <-time.After(time.Duration(cli.params.EpochMillis) * time.Millisecond):
			e := errors.New(fmt.Sprintf("client(%v): server reply timeout, current remain epoch=%v\n", cli.clientId, cli.remainEpoch-1))
			fmt.Printf(e.Error())
			cli.mu.Lock()
			cli.remainEpoch--
			if cli.remainEpoch == 0 {
				cli.mu.Unlock()
				fmt.Printf("client(%v) [handleStuff] exits due to time out, connection(%v) is lost.\n", cli.clientId, cli.ConnID())
				cli.chanReadFailure <- true
				cli.Close()
				return
			}
			cli.mu.Unlock()
			if cli.latestReceivedDataMsg.Length() == 0 && !checkRemainedUnAckMsg(cli) { // [2] has not been received since connected. first ack msg won't be cached.
				sendConnAckMsg(cli)
			} else if checkRemainedUnAckMsg(cli) { // [3] data message remaines to ack.
				reSendLatestUnAckDataMsg(cli)
			} else { // [4] resend an acknowledgment message for each of the last ω data message that have been received.
				reSendLatestAckMsg(cli)
			}
		}
	}
	fmt.Printf("client(%v) [handleStuff] exits due to time out, connection(%v) is lost.\n", cli.clientId, cli.ConnID())
	cli.chanReadFailure <- true
	cli.Close()
}

func doRead(cli *client) {
	fmt.Printf("client(%v) begin to ready received data from cached window.\n", cli.clientId)
	cli.mu.Lock()
	defer cli.mu.Unlock()
	for item := cli.latestReceivedDataMsg.First(); item != nil; item = cli.latestReceivedDataMsg.Next() {
		if item.SeqNum < cli.nextReceiveDataSeqNum {
			continue
		}
		if item.SeqNum == cli.nextReceiveDataSeqNum {
			cli.chanMsgData <- item
			cli.nextReceiveDataSeqNum++
		} else {
			break
		}
	}
}

func checkRemainedUnAckMsg(cli *client) bool {
	fmt.Printf("client(%v) checks reamained Un-Ack messages.\n", cli.clientId)
	cli.mu.Lock()
	defer cli.mu.Unlock()
	for item := cli.latestSentDataMsg.First(); item != nil; item = cli.latestSentDataMsg.Next() {
		if !item.ack {
			return true
		}
	}
	return false
}

func reSendLatestAckMsg(cli *client) {
	fmt.Printf("client(%v): resend latest ack messages.\n", cli.clientId)
	cli.mu.Lock()
	defer cli.mu.Unlock()
	for item := cli.latestReceivedDataMsg.First(); item != nil; item = cli.latestReceivedDataMsg.Next() {
		ackMsg := NewAck(cli.ConnID(), item.SeqNum)
		go onWrite(cli, ackMsg)
	}

}

func reSendLatestUnAckDataMsg(cli *client) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	fmt.Printf("client(%v): resend latest unAck data messsages to server(%v).\n", cli.clientId, cli.hostport)
	for item := cli.latestSentDataMsg.First(); item != nil; item = cli.latestSentDataMsg.Next() {
		if item.ack { // already ack
			continue
		}
		go onWrite(cli, item.msg)
	}
}

func sendConnAckMsg(cli *client) {
	conAckMsg := NewAck(cli.ConnID(), 0)
	fmt.Printf("client(%v): send connection request ack(0) to server(%v).\n", cli.clientId, cli.hostport)
	go onWrite(cli, conAckMsg)
}

// including old discarded message and handled messages lied in sliding window.
func (c *client) checkMsgSeqDuplicate(msg *Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("client(%v) checks whether data messsage(%v) from server(%v) is duplicate.\n", c.clientId, msg.String(), c.hostport)
	switch msg.Type {
	case MsgData:
		if msg.SeqNum < c.nextReceiveDataSeqNum {
			fmt.Printf("client(%v): message(%v) is already discarded!\n", c.clientId, msg.String())
			return true
		}
		// if item := c.latestReceivedDataMsg.First(); item != nil {
		// 	if msg.SeqNum < item.SeqNum { // already discarded
		// 		fmt.Printf("client(%v): message(%v) is already discarded!\n", c.clientId, msg.String())
		// 		return true
		// 	}
		// }
		for item := c.latestReceivedDataMsg.First(); item != nil; item = c.latestReceivedDataMsg.Next() { // ack in sliding window
			if item.SeqNum == msg.SeqNum {
				fmt.Printf("client(%v): message(%v) is already ack(cache in sliding window)!\n", c.clientId, msg.String())
				return true
			}
		}
	case MsgAck:
		if item := c.latestSentDataMsg.First(); item != nil {
			if msg.SeqNum < item.msg.SeqNum { // already discarded
				fmt.Printf("client(%v): message(%v) is already discarded!\n", c.clientId, msg.String())
				return true
			}
		}
		for item := c.latestSentDataMsg.First(); item != nil; item = c.latestSentDataMsg.Next() { // ack in sliding window
			if item.msg.SeqNum == msg.SeqNum && item.ack {
				fmt.Printf("client(%v): message(%v) is already ack(cache in sliding window)!\n", c.clientId, msg.String())
				return true
			}
		}
	}
	fmt.Printf("client(%v): message(%v) is legal!\n", c.clientId, msg.String())
	return false
}

func handleRequestConn(cli *client) {
	var e error
	for cli.remainEpoch > 0 {
		select {
		case <-cli.chanConnClose:
			fmt.Printf("client(%v): connection(%v) has been closed, exit..\n", cli.clientId, cli.ConnID())
			return
		case <-cli.chanConBuild:
			for requestConnection(cli) != nil {
				// fmt.Printf(err.Error())
				time.Sleep(RETRY_READ_WRITE_INTERVAL * time.Millisecond)
			}
			readBytes := make([]byte, BUFFER_SIZE)
			var n int
			var err error
			n, err = cli.conn.Read(readBytes)
			// retry if error occurs when read or write connection message.
			for err != nil {
				fmt.Printf("client(%v): read error occurs: %v.\n", cli.clientId, err.Error())
				e = err
				fmt.Printf("client(%v): sleep for %v millisecond, and retry.\n", cli.clientId, RETRY_READ_WRITE_INTERVAL)
				for requestConnection(cli) != nil {
					time.Sleep(RETRY_READ_WRITE_INTERVAL * time.Millisecond)
				}
				n, err = cli.conn.Read(readBytes)
				time.Sleep(RETRY_READ_WRITE_INTERVAL * time.Millisecond)
			}
			// read success.
			cli.mu.Lock()
			var ackReqConn Message
			json.Unmarshal(readBytes[:n], &ackReqConn) // type check
			fmt.Printf("client(%v): receive message(%v).\n", cli.clientId, ackReqConn.String())
			// should the ack put into the queue ?
			if ackReqConn.Type != MsgAck {
				cli.remainEpoch--
				e = errors.New("client: Data reply type error..")
				cli.chanConBuild <- true
				cli.mu.Unlock()
				continue
			}
			cli.connectionId = ackReqConn.ConnID
			cli.receivedAckSeqNum = 0
			cli.chanConBuildSuccess <- true
			fmt.Printf("client build connection with server successfully.\n")
			resetRemainEpoch(cli)
			cli.mu.Unlock()
			return // success
		case <-time.After(time.Duration(cli.params.EpochMillis) * time.Millisecond):
			cli.mu.Lock()
			e = errors.New(fmt.Sprintf("client(%v): server reply timeout..", cli.clientId))
			cli.remainEpoch--
			cli.chanConBuild <- true
			cli.mu.Unlock()
		}
	}
	fmt.Println(e)
}

func requestConnection(cli *client) error {
	fmt.Printf("client(%v) begin to write Connect Msg to server(%v) by connection(%v).\n", cli.clientId, cli.hostport, cli.ConnID())
	conMsg := NewConnect()
	if err := onWrite(cli, conMsg); err != nil {
		fmt.Printf("Client(%v) connection msg write error: %v\n", cli.clientId, err.Error())
		return err
	}
	return nil
}

func (c *client) checkClose(err error) bool {
	fmt.Println("client check whether connection has been closed.")
	return err == io.EOF
}

// connect to server when given a specific host with port.
func connect(hostport string) (*lspnet.UDPConn, error) {
	fmt.Printf("client begin to connect to server(%v).\n", hostport)
	if addr, err := lspnet.ResolveUDPAddr("udp", hostport); err != nil {
		return nil, err
	} else if conn, err := lspnet.DialUDP("udp", nil, addr); err != nil {
		return nil, err
	} else {
		fmt.Printf("client connected to server(%v).\n", hostport)
		return conn, nil
	}
}

// this method will write bytes using the whole message.
func onWrite(cli *client, dataMsg *Message) error {
	fmt.Printf("client(%v) begin to write Msg(%v) to server(%v) by connection(%v).\n", cli.clientId, dataMsg.String(), cli.hostport, cli.ConnID())
	mdBytes, _ := json.Marshal(dataMsg)
	if _, err := cli.conn.Write(mdBytes); err != nil { // retry or close connection ?
		fmt.Println("Client: [onWrite] error: ", err)
		return err
		// } else if n != len(mdBytes) {
		// 	errMsg := fmt.Sprintf("client write error, return length is %v expected %v.\n", n, len(mdBytes))
		// 	fmt.Printf(errMsg)
		// 	return errors.New(errMsg)
	}
	if dataMsg.Type == MsgData {
		fmt.Printf("client write over for dataMsg(%v).\n", string(dataMsg.Payload))
	} else {
		fmt.Printf("client write over for msg(%v).\n", dataMsg.String())
	}
	return nil
}

func checkPendingMsg(callMethod string, cli *client) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	select {
	case <-cli.chanConnOnClose:
		fmt.Printf("[%v] client(%v) begin to check pending msg.\n", callMethod, cli.clientId)
		if item := cli.latestSentDataMsg.Last(); item != nil {
			fmt.Printf("[%v] client(%v): connection(%v), last message seqNum=%v, cc.receivedAckSeqNum=%v.\n", callMethod, cli.clientId, cli.ConnID(), item.msg.SeqNum, cli.receivedAckSeqNum)
			fmt.Printf("[%v] client(%v): connection(%v), len(cli.chanWriteMsgCache)=%v.\n", callMethod, cli.clientId, cli.ConnID(), len(cli.chanWriteMsgCache))
			if item.msg.SeqNum == cli.receivedAckSeqNum && len(cli.chanWriteMsgCache) == 0 {
				fmt.Printf("[%v] client(%v): connection(%v) maintained messages have been done, hence can exit.\n", callMethod, cli.clientId, cli.ConnID())
				cli.chanMsgDone <- true
				return
			}
		} else {
			// empty latestSentDataMsg.
			fmt.Printf("[%v] client(%v): connection(%v) has no messages to handled, hence can exit.\n", callMethod, cli.clientId, cli.ConnID())
			cli.chanMsgDone <- true
			return
		}
		if len(cli.chanConnOnClose) == 0 {
			cli.chanConnOnClose <- true
		}
		fmt.Printf("[%v] client(%v) checked pending msg over.\n", callMethod, cli.clientId)
	default:
	}

}

func (c *client) ConnID() int {
	return c.connectionId
}

func (c *client) onRead() {
	for {
		fmt.Printf("client(%v): continue to read message bytes.\n", c.clientId)
		select {
		case <-c.chanConnClose:
			fmt.Printf("client(%v): close connection(%v).\n", c.clientId, c.ConnID())
			return
		default:
		}
		readBytes := make([]byte, BUFFER_SIZE)
		if n, err := c.conn.Read(readBytes); err != nil {
			continue
		} else {
			fmt.Printf("client(%v): read message bytes successfully.\n", c.clientId)
			readMsg := &Message{}
			json.Unmarshal(readBytes[:n], readMsg)
			switch readMsg.Type {
			case MsgConnect:
				fmt.Printf("client: Error message type: MsgConnect(Duplicate?).\n")
			case MsgData:
				c.chanReply <- readMsg
			case MsgAck:
				if c.checkMsgSeqDuplicate(readMsg) {
					fmt.Printf("client(%v): msg(%v) is duplicate, will not trigger time events.\n", c.clientId, readMsg.String())
					continue
				}
				c.chanReplyAck <- readMsg
			}
		}
	}
}

func (c *client) Read() ([]byte, error) {
	for {
		select {
		case <-c.chanConnClose:
			return nil, errors.New(fmt.Sprintf("connection(%v) closed", c.ConnID()))
		case message := <-c.chanMsgData:
			fmt.Printf("=========client(%v) read data(%v) from server(%v) by connection(%v).\n", c.clientId, string(message.Payload), c.hostport, c.ConnID())
			return message.Payload, nil
		}
	}
}

func checkSlidingWindowIdle(cli *client) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	fmt.Printf("client(%v) begin to check sliding window.\n", cli.clientId)
	for i := 0; i < len(cli.chanSWIdle); i++ {
		<-cli.chanSWIdle
	}
	if item := cli.latestSentDataMsg.Last(); item != nil {
		fmt.Printf("client(%v) receivedAckSeqNum=%v, lastMsgSeqNum=%v, WindowSize=%v.\n", cli.clientId, cli.receivedAckSeqNum, item.msg.SeqNum, cli.params.WindowSize)
		for i := 0; i < cli.params.WindowSize-(item.msg.SeqNum-cli.receivedAckSeqNum); i++ {
			cli.chanSWIdle <- true
		}
		return
	}
	if cli.latestSentDataMsg.Length() == 0 {
		fmt.Printf("client(%v) in initial state, receivedAckSeqNum=%v, WindowSize=%v.\n", cli.clientId, cli.receivedAckSeqNum, cli.params.WindowSize)
		for i := 0; i < cli.params.WindowSize; i++ {
			cli.chanSWIdle <- true
		}
		return
	}
}

func doWrite(cli *client) {
	fmt.Printf("client(%v): continuing run doWrite goroutine..\n", cli.clientId)
	for payload := range cli.chanWriteMsg {
		fmt.Printf("Client(%v) check whether message should block due to full sliding window before Write called.\n", cli.clientId)
		checkSlidingWindowIdle(cli)
		select {
		case <-cli.chanSWIdle:
		}
		fmt.Printf("Client(%v) message can be Written.\n", cli.clientId)
		var dataMsg *Message
		if payload != nil && len(payload) != 0 {
			cli.mu.Lock()
			dataMsg = NewData(cli.ConnID(), cli.nextSendDataSeqNum, payload)
			cli.nextSendDataSeqNum++
			cli.mu.Unlock()
			saveSentMsg(cli, dataMsg)
			go onWrite(cli, dataMsg)
		}
		<-cli.chanWriteMsgCache
	}
}

func (c *client) Write(payload []byte) error {
	fmt.Printf("Client(%v) Write called..\n", c.clientId)
	select {
	case <-c.chanConnClose:
		fmt.Printf("connection(%v) has been closed..\n", c.ConnID())
		return errors.New("connection closed")
	default:
	}
	// go doWrite(c, payload)
	c.chanWriteMsg <- payload
	c.chanWriteMsgCache <- payload
	return nil
}

// block until all all pending messages have been sent.
func (c *client) Close() error {
	fmt.Printf("Client(%v): try to close connection(%v).\n", c.clientId, c.ConnID())
	select {
	case <-c.chanReadFailure:
		fmt.Printf("client(%v): [Close] Connection(%v) read error due to server closing, so we won't check Un-Ack messages, exiting now at once.\n", c.clientId, c.ConnID())
		var err error
		for i := 0; i < 10; i++ {
			c.chanConnClose <- true
		}
		if err = c.conn.Close(); err != nil {
			c.checkClose(err)
		}
		fmt.Printf("Client(%v): connection(%v) exited.\n", c.clientId, c.ConnID())
		return err
	default:
	}
	c.chanConnOnClose <- true
	checkPendingMsg("Close", c)
	select {
	case <-c.chanMsgDone:
		var err error
		for i := 0; i < 10; i++ {
			c.chanConnClose <- true
		}
		if err = c.conn.Close(); err != nil {
			c.checkClose(err)
		}
		return err
	}
}
