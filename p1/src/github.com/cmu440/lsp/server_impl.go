// Contains the implementation of a LSP server.

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

const (
	CONN_BUFFERED_SIZE = 1024
	MAX_TRY_TIMES      = 10
	CHAN_SIZE_SERVERAL = 10
)

// per clinet(connection) per compConn instance.
type compConn struct {
	connID      int // connectin id of client
	remainEpoch int // epoch remained to retry operations.

	nextSendDataSeqNum    int // Sequence Number of Data Message to send next.
	receivedAckSeqNum     int // the highest sequence number of acked messages.
	nextReceiveDataSeqNum int // the next sequence number of data messages that expect to receive.

	latestSentDataMsg     *MessageWindowSent // maintain a sliding window for their w most recently sent data message for re-sending at a later time.
	latestReceivedDataMsg *MessageWindow     // maintain a sliding window for their w most recently sent acknowledgments for re-sending at a later time.

	chanWriteMsg      chan []byte   // channel for messages written temporally.
	chanWriteMsgCache chan []byte   // channel caching all messages that must be sent before connection closing.
	chanConnClose     chan int      // signal to close client connection, hence all background goroutines will exit.
	chanReadFailure   chan int      // catch client connection read failure, hence the connection has lost.
	chanReply         chan *Message // channel for client receiving data messages.
	chanReplyAck      chan *Message // channel for client receiving data messages of Ack.
	chanReplyCon      chan *Message // channel for client receiving data messages of Connect.
	chanSWIdle        chan bool     // check whether the sliding window is idle, so that ensure any coming messages should be blocked or not.
	chanMsgDone       chan bool     // all pending messages, which including those sent already but not received ack and those are being written.

	remoteAddr *lspnet.UDPAddr // remote client address.

	cmu sync.Mutex
}

type server struct {
	// TODO: implement this!
	port     int
	params   *Params // parameters of lsp
	mu       sync.Mutex
	chanExit chan bool // channel for server exiting, hence all connections will be closed before signal it.

	listener *lspnet.UDPConn // net connection with server

	chanMsgData             chan *Message // channel for store all messages that will send to clients.
	chanConnOnClose         chan bool     // signal to close some connection. used before chanConnClose.
	chanConnOnCloseNonBlock chan bool     // like the above one, but it will not block.
	chanOnListen            chan bool     // channel to signal to receive a new client connection.
	chanReadFailure         chan int      // server read failure when reading from some client connection.

	clientInfoMap map[string]int    // client remoteAddress => client connID, to identify those new client connections.
	chanRespMap   map[int]*compConn // connID => compConn, cache all client connection info.
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	fmt.Printf("Server: create server node.\n")
	s := &server{
		port:                    port,
		params:                  params,
		chanConnOnClose:         make(chan bool, CHAN_UNIT_SIZE),
		chanConnOnCloseNonBlock: make(chan bool, CHAN_UNIT_SIZE),
		chanExit:                make(chan bool, CHAN_SIZE_SERVERAL),
		chanReadFailure:         make(chan int, CHAN_SIZE_SERVERAL),
		chanMsgData:             make(chan *Message, CHAN_SIZE_MULTI),

		chanOnListen: make(chan bool, CHAN_UNIT_SIZE),

		chanRespMap:   make(map[int]*compConn, CONN_BUFFERED_SIZE), // connID -> compCon
		clientInfoMap: make(map[string]int),                        // remoteAddress -> connID
	}
	lspnet.EnableDebugLogs(true)
	go s.listen(port)
	select {
	case <-s.chanOnListen:
		go s.onRead()
	}
	return s, nil
}

func (s *server) initConn() int {

	cc := &compConn{
		connID:                getNextConnId(),
		remainEpoch:           s.params.EpochLimit,
		nextSendDataSeqNum:    1,
		receivedAckSeqNum:     0,
		nextReceiveDataSeqNum: 1,

		latestSentDataMsg:     NewMessageWindowSent(s.params.WindowSize),
		latestReceivedDataMsg: NewMessageWindow(s.params.WindowSize),

		chanReply:         make(chan *Message, CHAN_UNIT_SIZE),
		chanReplyAck:      make(chan *Message, CHAN_UNIT_SIZE),
		chanReplyCon:      make(chan *Message, CHAN_UNIT_SIZE),
		chanWriteMsg:      make(chan []byte, CHAN_SIZE_MULTI),
		chanWriteMsgCache: make(chan []byte, CHAN_SIZE_MULTI),
		chanConnClose:     make(chan int, CHAN_SIZE_SERVERAL),
		chanReadFailure:   make(chan int, CHAN_UNIT_SIZE),
		chanSWIdle:        make(chan bool, CHAN_SIZE_MULTI),
		chanMsgDone:       make(chan bool, CHAN_UNIT_SIZE),
	}
	// s.mu.Lock()
	s.chanRespMap[cc.connID] = cc
	// s.mu.Unlock()
	fmt.Printf("Server: init connection(%v).\n", cc.connID)
	fmt.Printf("Server: begin to run doWrite goroutine for connection(%v).\n.", cc.connID)
	go s.doWrite(cc.connID)
	return cc.connID
}

// listening at a specific port.
func (s *server) listen(port int) {
	fmt.Printf("Server: begin to listen connection.\n")
	if addr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", port)); err != nil {
		fmt.Println(err.Error())
	} else {
		if conn, err := lspnet.ListenUDP("udp", addr); err != nil || conn == nil {
			fmt.Println(err.Error())
			return
		} else {
			s.listener = conn
			fmt.Printf("Server: listened connection.\n")
			s.chanOnListen <- true
		}
	}
}

func (s *server) handStuff(connID int) {
	fmt.Printf("server: begin to handle stuff for connection(%v).\n", connID)
	cc, ok := s.chanRespMap[connID]
	if !ok {
		fmt.Println(errors.New("connection closed"))
		return
	}
	for cc.remainEpoch > 0 {
		select {
		case <-s.chanExit:
			fmt.Printf("server: [handStuff] exit now...\n")
			return
		case <-cc.chanConnClose:
			fmt.Printf("server: [handStuff] connection(%v) is closing..\n", connID)
			return
		case <-cc.chanReplyCon:
			// need not to reset epoch.
			s.sendConnAckMsg(connID)
			// s.chanMsgData <- readMsg
		case readMsg := <-cc.chanReply:
			// fmt.Printf("server: [1] receive Data message(%v).\n", readMsg.String())
			s.resetRemainEpoch(readMsg)
			duplicate := s.checkMsgSeqDuplicate(readMsg)
			if duplicate {
				fmt.Printf("server: msg(%v) is duplicate, will not trigger time events.\n", readMsg.String())
			}
			s.saveMsgAndAck(readMsg, duplicate)
			// if !duplicate {
			// 	s.chanMsgData <- readMsg
			// }
			s.doRead(readMsg.ConnID)
		case readMsg := <-cc.chanReplyAck:
			// fmt.Printf("server: [2] receive Ack message(%v).\n", readMsg.String())
			s.resetRemainEpoch(readMsg)
			s.updateLatestSentDataMsg(readMsg)
			// s.chanMsgData <- readMsg
		case <-time.After(time.Duration(s.params.EpochMillis) * time.Millisecond):
			e := errors.New(fmt.Sprintf("server: connection(%v) reply timeout, current epoch=%v..\n", connID, cc.remainEpoch-1))
			fmt.Printf(e.Error())
			s.mu.Lock()
			cc.remainEpoch--
			if cc.remainEpoch == 0 {
				fmt.Printf("Server: read from client fail, exit from connection(%v).\n", connID)
				s.chanReadFailure <- connID
				// cc.chanReadFailure <- connID
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			// [2] has not been received since connected. first ack msg won't be cached.
			if cc.latestReceivedDataMsg.Length() == 0 && !s.checkRemainedUnAckMsg(connID) {
				s.sendConnAckMsg(connID)
			} else if s.checkRemainedUnAckMsg(connID) { // [3] data message remaines to ack.
				s.reSendLatestUnAckDataMsg(connID)
			} else { // [4] resend an acknowledgment message for each of the last ω data message that have been received.
				s.reSendLatestAckMsg(connID)
			}
		}
	}
}

func (s *server) doRead(connID int) {
	fmt.Printf("server begin to ready received data from cached window for connection(%v).\n", connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	cc := s.chanRespMap[connID]
	for item := cc.latestReceivedDataMsg.First(); item != nil; item = cc.latestReceivedDataMsg.Next() {
		if item.SeqNum < cc.nextReceiveDataSeqNum {
			continue
		}
		if item.SeqNum == cc.nextReceiveDataSeqNum {
			s.chanMsgData <- item
			cc.nextReceiveDataSeqNum++
		} else {
			break
		}
	}
}

func (s *server) onRead() {
	fmt.Printf("server: continue to listen to message.\n")
	for {
		select {
		case <-s.chanExit:
			fmt.Printf("server: [onRead] exiting.\n")
			return
		default:
		}
		readBuf := make([]byte, BUFFER_SIZE)
		if n, remoteAddr, err := s.listener.ReadFromUDP(readBuf); err != nil {
			fmt.Println(fmt.Sprintf("server: connection read error: %v", err.Error()))
		} else {
			fmt.Printf("server: obtain remote address info(%v) from message result.\n", remoteAddr)
			s.mu.Lock()
			connID, ok := s.clientInfoMap[remoteAddr.String()]
			if !ok {
				connID = s.initConn()
				s.clientInfoMap[remoteAddr.String()] = connID
				fmt.Printf("server: cache new client info connID=%v, remoteAdd=%v.\n", connID, remoteAddr)
				s.mu.Unlock()
				go s.handStuff(connID)
			} else {
				s.mu.Unlock()
			}
			s.mu.Lock()
			cc, ok2 := s.chanRespMap[connID]
			if ok2 {
				s.chanRespMap[connID].remoteAddr = remoteAddr
				s.mu.Unlock()
			} else {
				fmt.Printf("server: [onRead] connection(%v) info has lost.\n", connID)
				s.mu.Unlock()
				return
			}
			fmt.Printf("server: read message from remoteAddr(%v) successfully.\n", s.chanRespMap[connID].remoteAddr)
			readMsg := &Message{}
			json.Unmarshal(readBuf[:n], readMsg) // type check ?
			fmt.Printf("server: receive message(%v).\n", readMsg.String())
			switch readMsg.Type {
			case MsgConnect:
				fmt.Printf("server: [0] receive Connection message(%v).\n", readMsg.String())
				cc.chanReplyCon <- readMsg
			case MsgData:
				fmt.Printf("server: [1] receive Data message(%v).\n", readMsg.String())
				// if s.checkMsgSeqDuplicate(readMsg) {
				// 	continue
				// }
				cc.chanReply <- readMsg
			case MsgAck:
				fmt.Printf("server: [2] receive Ack message(%v).\n", readMsg.String())
				if s.checkMsgSeqDuplicate(readMsg) {
					continue
				}
				cc.chanReplyAck <- readMsg
			}
		}
	}
}

func (s *server) reSendLatestAckMsg(connID int) {
	fmt.Printf("Server begin to resend latest ack message by connection(%v).\n", connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	cc := s.chanRespMap[connID]
	for item := cc.latestReceivedDataMsg.First(); item != nil; item = cc.latestReceivedDataMsg.Next() {
		ackMsg := NewAck(connID, item.SeqNum)
		go s.onWrite(connID, ackMsg)
	}
}

func (s *server) reSendLatestUnAckDataMsg(connID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cc := s.chanRespMap[connID]
	fmt.Printf("server resends latest unAck data messsages to connection(%v).\n", connID)
	for item := cc.latestSentDataMsg.First(); item != nil; item = cc.latestSentDataMsg.Next() {
		if item.ack { // already ack
			continue
		}
		go s.onWrite(connID, item.msg)
	}
}

func (s *server) checkRemainedUnAckMsg(connID int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cc := s.chanRespMap[connID]
	for item := cc.latestSentDataMsg.First(); item != nil; item = cc.latestSentDataMsg.Next() {
		if !item.ack {
			return true
		}
	}
	return false
}

func (s *server) sendConnAckMsg(connID int) {
	conAckMsg := NewAck(connID, 0)
	fmt.Printf("server send connection request ack(0) to connection(%v).\n", connID)
	go s.onWrite(connID, conAckMsg)
}

// this method will write bytes using the whole message.
// if not block, then will new a goroutine, and pass results by channel.
func (s *server) onWrite(connID int, dataMsg *Message) {
	fmt.Printf("server begin to write Msg(%v) to connection(%v).\n", dataMsg.String(), connID)
	mdBytes, _ := json.Marshal(dataMsg)
	s.mu.Lock()
	cc, ok := s.chanRespMap[connID]
	if !ok {
		fmt.Printf("server: [onWrite] connection(%v) info has lost.\n", connID)
		s.mu.Unlock()
		return
	}
	addr := cc.remoteAddr
	if ok {
		s.mu.Unlock()
	}
	if _, err := s.listener.WriteToUDP(mdBytes, addr); err != nil {
		fmt.Println(err.Error())
		return
		// } else if n != len(mdBytes) {
		// 	errMsg := fmt.Sprintf("server write error, return length is %v expected %v.\n", n, len(mdBytes))
		// 	fmt.Printf(errMsg)
		// 	return errors.New(errMsg)
	}
	if dataMsg.Type == MsgData {
		fmt.Printf("server write over for dataMsg(%v).\n", string(dataMsg.Payload))
	}
}

func (s *server) updateLatestSentDataMsg(msg *Message) {
	s.mu.Lock()
	cc := s.chanRespMap[msg.ConnID]
	ack := false
	for item := cc.latestSentDataMsg.First(); item != nil; item = cc.latestSentDataMsg.Next() {
		if item.msg.SeqNum == msg.SeqNum {
			if item.ack {
				fmt.Printf("server receive a duplicate ack(%v) message.\n", msg.SeqNum)
			} else {
				item.ack = true
				ack = true
			}
			break
		}
	}
	tmp := cc.receivedAckSeqNum
	for item := cc.latestSentDataMsg.First(); item != nil; item = cc.latestSentDataMsg.Next() {
		if !item.ack {
			break
		}
		cc.receivedAckSeqNum = item.msg.SeqNum
	}
	fmt.Printf("server update its receivedAckSeqNum from %v to %v for connection(%v).\n", tmp, cc.receivedAckSeqNum, msg.ConnID)
	s.mu.Unlock()
	if ack {
		go s.checkPendingMsg("updateLatestSentDataMsg", msg.ConnID)
		go s.checkPendingMsgNonBlock("updateLatestSentDataMsg", msg.ConnID)
		go s.checkSlidingWindowIdle(msg.ConnID)
	}
}

func (s *server) saveMsgAndAck(msg *Message, duplicate bool) error {
	// put msg to sliding window.
	// messages in sliding window will be sorted by sequence number once a message is put into.
	s.mu.Lock()
	if !duplicate {
		s.chanRespMap[msg.ConnID].latestReceivedDataMsg.Put(msg)
		fmt.Printf("server: save received the new message(%v) for connection(%v).\n", msg.String(), msg.ConnID)
	}
	// send ack although it may be duplicate.
	ackMsg := NewAck(msg.ConnID, msg.SeqNum)
	s.mu.Unlock()
	fmt.Printf("Server begin to save data msg(%v) and reply ack(%v) by connection(%v).\n", msg.String(), ackMsg.String(), msg.ConnID)
	go s.onWrite(msg.ConnID, ackMsg)
	return nil
}

func (s *server) saveSentMsg(msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("server save sent msg(%v) for connection(%v).\n", msg.String(), msg.ConnID)
	swmg := &SWMessage{
		ack: false,
		msg: msg,
	}
	s.chanRespMap[msg.ConnID].latestSentDataMsg.Put(swmg)
	fmt.Printf("server saved sent msg(%v) for connection(%v) and returned\n.", msg.String(), msg.ConnID)
}

func (s *server) resetRemainEpoch(msg *Message) {
	s.mu.Lock()
	fmt.Printf("server resets its remained epoch to %v for connection(%v).\n", s.chanRespMap[msg.ConnID].remainEpoch, msg.ConnID)
	s.chanRespMap[msg.ConnID].remainEpoch = s.params.EpochLimit
	s.mu.Unlock()
}

// including old discarded message and handled messages lied in sliding window.
func (s *server) checkMsgSeqDuplicate(msg *Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("server checks whether data messsage(%v) from connection(%v) is duplicate.\n", msg.String(), msg.ConnID)
	cc, ok := s.chanRespMap[msg.ConnID]
	if !ok {
		fmt.Printf(errors.New(fmt.Sprintf("server: [checkMsgSeqDuplicate] connection(%v) closed", msg.ConnID)).Error())
		return false
	}
	switch msg.Type {
	case MsgData:
		if msg.SeqNum < cc.nextReceiveDataSeqNum {
			fmt.Printf("server: message(%v) is already discarded!\n", msg.String())
			return true
		}
		// if item := cc.latestReceivedDataMsg.First(); item != nil {
		// 	if msg.SeqNum < item.SeqNum { // already discarded
		// 		fmt.Printf("server: message(%v) is already discarded!\n", msg.String())
		// 		return true
		// 	}
		// }
		for item := cc.latestReceivedDataMsg.First(); item != nil; item = cc.latestReceivedDataMsg.Next() { // ack in sliding window
			if item.SeqNum == msg.SeqNum {
				fmt.Printf("server: message(%v) is already ack(cache in sliding window)!\n", msg.String())
				return true
			}
		}
	case MsgAck:
		if item := cc.latestSentDataMsg.First(); item != nil {
			if msg.SeqNum < item.msg.SeqNum { // already discarded
				fmt.Printf("server: message(%v) is already discarded!\n", msg.String())
				return true
			}
		}
		for item := cc.latestSentDataMsg.First(); item != nil; item = cc.latestSentDataMsg.Next() { // ack in sliding window
			if item.msg.SeqNum == msg.SeqNum && item.ack {
				fmt.Printf("server: message(%v) is already ack(cache in sliding window)!\n", msg.String())
				return true
			}
		}
	}
	fmt.Printf("server: message(%v) is legal!\n", msg.String())
	return false
}

func (s *server) Read() (int, []byte, error) {
	for {
		select {
		case <-s.chanExit:
			fmt.Printf("server: [Read] exit now...\n")
			return -1, nil, errors.New(fmt.Sprintf("server exit....\n"))
		case cid := <-s.chanReadFailure:
			fmt.Printf("server: read from connection(%v) fail.\n", cid)
			cc, ok := s.chanRespMap[cid]
			if ok {
				cc.chanReadFailure <- cid
			} else {
				fmt.Printf("server: [Read] connection(%v) closed, but the server may not detect it.\n", cid)
			}
			return cid, nil, errors.New(fmt.Sprintf("server read from connection(%v) fail.\n", cid))
		case message := <-s.chanMsgData:
			fmt.Printf("=========server: read data(%v) from client by connection(%v).\n", string(message.Payload), message.ConnID)
			return message.ConnID, message.Payload, nil
		}
	}
}

func (s *server) checkClose(err error) bool {
	return err == io.EOF
}

func (s *server) doWrite(connID int) {
	fmt.Printf("Server: continuing run doWrite goroutine for connection(%v)..\n", connID)
	s.mu.Lock()
	cc, ok := s.chanRespMap[connID]
	if !ok {
		fmt.Printf(errors.New(fmt.Sprintf("server: [doWrite] connection(%v) closed", connID)).Error())
		s.mu.Unlock()
		return
	} else {
		s.mu.Unlock()
	}
	for payload := range cc.chanWriteMsg {
		select {
		case <-cc.chanConnClose:
			fmt.Printf("server: [doWrite] connection(%v) is closing..\n", connID)
			return
		default:
		}
		fmt.Printf("server check whether message should be block due to full sliding window before Write called.\n")
		s.checkSlidingWindowIdle(connID)
		select {
		case <-cc.chanSWIdle:
		}
		fmt.Printf("server message can be Written.\n")
		var dataMsg *Message
		if payload != nil && len(payload) != 0 {
			s.mu.Lock()
			// cc, ok := s.chanRespMap[connID]
			// if !ok {
			// 	fmt.Printf(errors.New(fmt.Sprintf("server: [doWrite] connection(%v) closed", connID)).Error())
			// 	s.mu.Unlock()
			// 	return
			// }
			dataMsg = NewData(connID, cc.nextSendDataSeqNum, payload)
			s.chanRespMap[connID].nextSendDataSeqNum++
			s.mu.Unlock()
			s.saveSentMsg(dataMsg)
			go s.onWrite(connID, dataMsg)
		}
		<-cc.chanWriteMsgCache
	}
}

func (s *server) Write(connID int, payload []byte) error {
	fmt.Println("server: Write called...")
	s.mu.Lock()
	cc, ok := s.chanRespMap[connID]
	if !ok {
		return errors.New(fmt.Sprintf("server: [Write] connection(%v) info has lost", connID))
	}
	s.mu.Unlock()
	select {
	case <-cc.chanConnClose:
		fmt.Printf("server: [Write] connection(%v) is closing..\n", connID)
		return errors.New("connection closed")
	default:
	}
	// go s.doWrite(cc, connID, payload)
	cc.chanWriteMsg <- payload
	cc.chanWriteMsgCache <- payload
	fmt.Printf("Server: [Write] payload has been added to the write channel, so it must be writen.len(cc.chanWriteMsg)=%v.\n", len(cc.chanWriteMsg))
	return nil
}

func (s *server) checkSlidingWindowIdle(connID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cc, ok := s.chanRespMap[connID]
	if !ok {
		fmt.Printf("server: [checkSlidingWindowIdle] connection(%v) info has lost.\n", connID)
		return
	}
	for i := 0; i < len(cc.chanSWIdle); i++ {
		<-cc.chanSWIdle
	}
	if item := cc.latestSentDataMsg.Last(); item != nil {
		fmt.Printf("server receivedAckSeqNum=%v, lastMsgSeqNum=%v, WindowSize=%v.\n", cc.receivedAckSeqNum, item.msg.SeqNum, s.params.WindowSize)
		for i := 0; i < s.params.WindowSize-(item.msg.SeqNum-cc.receivedAckSeqNum); i++ {
			cc.chanSWIdle <- true
		}
		return
	}
	if cc.latestSentDataMsg.Length() == 0 {
		fmt.Printf("server in initial state, receivedAckSeqNum=%v, WindowSize=%v.\n", cc.receivedAckSeqNum, s.params.WindowSize)
		for i := 0; i < s.params.WindowSize; i++ {
			cc.chanSWIdle <- true
		}
		return
	}
}

func (s *server) checkPendingMsgNonBlock(callMethod string, connID int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cc, ok := s.chanRespMap[connID]
	if !ok {
		fmt.Printf("server: [checkPendingMsgNonBlock] connection(%v) info has lost.\n", connID)
		return false // equivalent to message done ?
	}
	select {
	case <-s.chanConnOnCloseNonBlock:
		fmt.Printf("[%v] server: begin to check Non-block pending msg for connection(%v).\n", callMethod, connID)
		if item := cc.latestSentDataMsg.Last(); item != nil {
			fmt.Printf("[%v] server: Non-block connection(%v), last message seqNum=%v, cc.receivedAckSeqNum=%v.\n", callMethod, connID, item.msg.SeqNum, cc.receivedAckSeqNum)
			fmt.Printf("[%v] server: Non-block connection(%v), len(cc.chanWriteMsgCache)=%v.\n", callMethod, connID, len(cc.chanWriteMsgCache))
			if item.msg.SeqNum == cc.receivedAckSeqNum && len(cc.chanWriteMsgCache) == 0 {
				// messages in sliding window and messages writen but havn't been saved in window.
				return false
			}
		}
		if len(s.chanConnOnCloseNonBlock) == 0 {
			s.chanConnOnCloseNonBlock <- true
		}
	default:
		return true
	}
	return true
}

func (s *server) checkPendingMsg(callMethod string, connID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cc, ok := s.chanRespMap[connID]
	if !ok {
		fmt.Printf("[%v] server: connection(%v) info has lost.\n", callMethod, connID)
		return // equivalent to message done ?
	}
	select {
	case <-s.chanConnOnClose:
		fmt.Printf("[%v] server: begin to check pending msg for connection(%v).\n", callMethod, connID)
		if item := cc.latestSentDataMsg.Last(); item != nil {
			fmt.Printf("[%v] server: connection(%v), last message seqNum=%v, cc.receivedAckSeqNum=%v.\n", callMethod, connID, item.msg.SeqNum, cc.receivedAckSeqNum)
			fmt.Printf("[%v] server: connection(%v), len(cc.chanWriteMsgCache)=%v.\n", callMethod, connID, len(cc.chanWriteMsgCache))
			if item.msg.SeqNum == cc.receivedAckSeqNum && len(cc.chanWriteMsgCache) == 0 {
				// messages in sliding window and messages writen but havn't been saved in window.
				fmt.Printf("[%v] server: connection(%v) maintained messages have been done, hence can exit.\n", callMethod, connID)
				cc.chanMsgDone <- true
			}
		}
		if len(s.chanConnOnClose) == 0 {
			s.chanConnOnClose <- true
		}
		fmt.Printf("[%v] server: over check pending msg for connection(%v) over.\n", callMethod, connID)
	default:

	}
}

//All pending messages to the client should be sent and acknowledged.
// However, unlike Close, this method should NOT block.
func (s *server) CloseConn(connID int) error {
	s.mu.Lock()
	cc, ok := s.chanRespMap[connID]
	if ok {
		if len(s.chanConnOnCloseNonBlock) == 0 {
			s.chanConnOnCloseNonBlock <- true
		}
		s.mu.Unlock()
		if pendingMsg := s.checkPendingMsgNonBlock("[CloseConn]", connID); !pendingMsg {
			cc.chanConnClose <- connID
			s.mu.Lock()
			delete(s.chanRespMap, connID)
			fmt.Printf("server: [CloseConn] Connection(%v) close.\n", connID)
			s.mu.Unlock()
			return nil
		} else {
			errMsg := fmt.Sprintf("server: [CloseConn] connection(%v) has unAck messages, should not be closed.\n", connID)
			fmt.Printf(errMsg)
			return errors.New(errMsg)
		}
	}
	s.mu.Unlock()
	return errors.New(fmt.Sprintf("server: [CloseConn] Connection(%v) has been already closed.", connID))
}

// This method should block until all pending messages for each client are sent
// and acknowledged. If one or more clients are lost during this time, a non-nil
// error should be returned. Once it returns, all goroutines running in the
// background should exit.
func (s *server) Close() error {
	fmt.Printf("Server: try to close all connection..\n")
	s.mu.Lock()
	deletedMap := make(map[int]*compConn)
	for connID, cc := range s.chanRespMap {
		deletedMap[connID] = cc
	}
	s.mu.Unlock()
	for connID, cc := range s.chanRespMap {
		go func(cc *compConn, connID int, deletedMap map[int]*compConn) {
			if len(s.chanConnOnClose) == 0 {
				s.chanConnOnClose <- true
			}
			s.checkPendingMsg("Close", connID)
			select {
			case <-cc.chanMsgDone:
				fmt.Printf("server: [Close] [s.chanMsgDone] Connection(%v) Acked messages, exiting now.\n", connID)
				fmt.Printf("[%v] server: connection(%v), last message seqNum=%v, cc.receivedAckSeqNum=%v.\n", "Close", connID, cc.latestSentDataMsg.Last().msg.SeqNum, cc.receivedAckSeqNum)
				cc.chanConnClose <- connID
				if s.exit(deletedMap, connID) {
					return
				}
			case <-cc.chanReadFailure:
				fmt.Printf("server: [Close] [cc.chanReadFailure] Connection(%v) read error due to its closing, so we won't check Un-Ack messages, exiting now.\n", connID)
				cc.chanConnClose <- connID
				if s.exit(deletedMap, connID) {
					return
				}
			}
		}(cc, connID, deletedMap)

	}
	return nil
}

func (s *server) exit(deletedMap map[int]*compConn, connID int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(deletedMap, connID)
	fmt.Printf("server: [Close] after delete connID=%v, len(deletedMap)=%v.\n", connID, len(deletedMap))
	if 0 == len(deletedMap) {
		fmt.Printf("Server: [close] is exiting now...\n")
		for i := 0; i < CHAN_SIZE_SERVERAL; i++ {
			s.chanExit <- true
		}
		fmt.Printf("Server: [Close] exit...\n")
		for k, _ := range s.chanRespMap {
			delete(s.chanRespMap, k)
		}
		return true
	}
	return false
}
