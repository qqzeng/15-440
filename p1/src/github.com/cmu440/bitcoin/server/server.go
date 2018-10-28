package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

const (
	pool_size       = 1024
	chan_size_small = 3
)

// client reqeust struct
type commonMesg struct {
	connID  int
	mesg    *bitcoin.Message
	isClose bool
}

type clientRequest struct {
	requestID    int
	workMinerNum int
	overMinerNum int
	resultHash   uint64
	resultNonce  uint64
}

type clientPool struct {
	clientMap map[int]*clientRequest // client.connID => clientRequest
}

type TaskStatus int

const (
	Completed TaskStatus = iota
	Running
	Abort
)

type task struct {
	connID int // client.connID
	mesg   *bitcoin.Message
	status TaskStatus
}

type minerRequest struct {
	connID      int
	runningTask *task // current running task.
}

type minerPool struct {
	minerMap       map[int]*minerRequest // miner.connID => minerReqeust
	chanIdleMiners chan *minerRequest
	taskListMap    map[int]*list.List // client.connID => taskList
	//one client request corresponds to one map slot and one map slot corresponds to many tasks.
}

type serverNode struct {
	srv            lsp.Server
	chanExit       chan bool
	chanOnExit     chan bool
	chanCommonMesg chan *commonMesg
	cp             *clientPool
	mp             *minerPool
	params         *lsp.Params
	logger         *log.Logger
	lf             *os.File
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port error!")
		return
	}
	sn := createServerNode(port)
	go sn.readMesg()
	go sn.handleStuff()
	sn.exit()
}

func (sn *serverNode) readMesg() {
	defer sn.logger.Printf("[readMesg] Server is exiting.\n")
	for {
		select {
		case <-sn.chanOnExit:
			sn.chanExit <- true
			return
		default:
			connID, data, err := sn.srv.Read()
			closed := false
			if err != nil { // may be the connection lost.
				sn.logger.Println("Server received error during read.")
				closed = true
			}
			var mesg bitcoin.Message
			json.Unmarshal(data, &mesg)
			sn.logger.Printf("Server read message %s from node %d.\n", mesg.String(), connID)
			cm := &commonMesg{
				connID:  connID,
				mesg:    &mesg,
				isClose: closed,
			}
			sn.chanCommonMesg <- cm
			sn.logger.Println("server send request chanCommonMesg")
		}
	}
}

func (sn *serverNode) sendMinerRequest(connID int, mesg *bitcoin.Message) {
	sn.logger.Printf("server begins to send message to miner(%v).\n", connID)
	mdBytes, _ := json.Marshal(mesg)
	err := sn.srv.Write(connID, mdBytes)
	if err != nil { // if write fails, then assign this job to another miner.
		sn.logger.Printf("server fails to write message(%v) to miner(%v).\n", mesg.String(), connID)
	}
	sn.logger.Printf("server finishes sending message(%v) to miner(%v).\n", mesg.String(), connID)
}

func (sn *serverNode) sendClientResult(connID int, mesg *bitcoin.Message) {
	sn.logger.Printf("server begin to send message to miners.\n")
	mdBytes, _ := json.Marshal(mesg)
	if err := sn.srv.Write(connID, mdBytes); err != nil {
		sn.logger.Printf("server aborts sending result(%v) to client(%v) due to connection lost.\n", mesg.String(), connID)
		return
	}
}

func (sn *serverNode) cacheMinerMesg(cm *commonMesg) {
	mr := &minerRequest{connID: cm.connID, runningTask: nil}
	sn.mp.minerMap[cm.connID] = mr
	sn.mp.chanIdleMiners <- mr
	sn.logger.Printf("Server cached Join message %s from minner(%d).\n", cm.mesg.String(), cm.connID)
}

func (sn *serverNode) removeClientRequest(connID int) {
	delete(sn.cp.clientMap, connID)
	sn.srv.CloseConn(connID)
}

func (sn *serverNode) recycleTask(connID int, mr *minerRequest) {
	if _, ok := sn.mp.minerMap[connID]; ok {
		delete(sn.mp.minerMap, connID)
	}
	if mr != nil && mr.runningTask != nil {
		cmID := mr.runningTask.connID
		if _, ok := sn.cp.clientMap[cmID]; ok {
			if _, ok1 := sn.mp.taskListMap[cmID]; ok1 {
				sn.mp.taskListMap[cmID].PushBack(mr.runningTask)
			}
		}
	}
}

// TODO: if there is no task, then it will loop forever.
func (sn *serverNode) assignTask(mr *minerRequest) {
	for _, taskList := range sn.mp.taskListMap {
		if e := taskList.Front(); e != nil {
			t := e.Value.(*task)
			mr.runningTask = t
			sn.sendMinerRequest(mr.connID, t.mesg)
			taskList.Remove(e)
			return
		}
	}
	// re-put the miner to idle miner channel.
	sn.mp.chanIdleMiners <- mr
}

func (sn *serverNode) distributeTasks(cm *commonMesg) *list.List {
	taskList := list.New()
	workMinerNum := len(sn.mp.minerMap)
	if workMinerNum == 0 {
		workMinerNum = bitcoin.DefaultTaskNum
	}
	avg := (cm.mesg.Upper - cm.mesg.Lower) / uint64(workMinerNum)
	if avg != 0 {
		i := 1
		start := cm.mesg.Lower
		end := start + avg
		for i = 1; i <= workMinerNum; i++ {
			if i == workMinerNum {
				end = cm.mesg.Upper + 1 // including upper number
			} else {
				end = start + avg
			}
			t := &task{
				connID: cm.connID,
				mesg:   bitcoin.NewRequest(cm.mesg.Data, start, end),
			}
			taskList.PushBack(t)
			start = end
		}
	} else {
		// assign to a signle miner or one nonce per miner.
		t := task{
			connID: cm.connID,
			mesg:   bitcoin.NewRequest(cm.mesg.Data, cm.mesg.Lower, cm.mesg.Upper),
		}
		taskList.PushBack(&t)
	}
	return taskList
}

func (sn *serverNode) handleClientMesg(cm *commonMesg) {
	tl := sn.distributeTasks(cm)
	sn.cp.clientMap[cm.connID] = &clientRequest{
		requestID:    bitcoin.GetNextRequestId(),
		resultHash:   ^uint64(0) - 1,
		resultNonce:  uint64(1),
		workMinerNum: tl.Len(),
		overMinerNum: 0,
	}
	sn.mp.taskListMap[cm.connID] = tl
	// select {
	// case <-sn.chanCliRequest:
	// 	sn.logger.Println("Server received new client request, and notify miner.")
	// default:
	// }
}

func (sn *serverNode) handleMinerResult(cm *commonMesg) {
	if mr, ok := sn.mp.minerMap[cm.connID]; ok {
		t := mr.runningTask
		mr.runningTask = nil // ready for new task.
		sn.mp.chanIdleMiners <- mr
		if cr, ok := sn.cp.clientMap[t.connID]; ok {
			cr.overMinerNum++
			if cr.resultHash > cm.mesg.Hash {
				cr.resultHash = cm.mesg.Hash
				cr.resultNonce = cm.mesg.Nonce
			}
			if cr.overMinerNum == cr.workMinerNum {
				rsltMesg := bitcoin.NewResult(cr.resultHash, cr.resultNonce)
				sn.sendClientResult(t.connID, rsltMesg)
				sn.removeClientRequest(t.connID)
			}
		}
	}
}

func (sn *serverNode) handleStuff() {
	defer sn.logger.Printf("[handleStuff] Server is exiting.\n")
	for {
		select {
		case <-sn.chanOnExit:
			sn.chanExit <- true
			return
		case cm := <-sn.chanCommonMesg:
			if cm.isClose {
				// client connection lost, aborting sending result.
				if _, ok := sn.cp.clientMap[cm.connID]; ok {
					sn.removeClientRequest(cm.connID)
				} else {
					// miner connection lost, recycle this task and re-assign it to another miner next.
					if mr, ok := sn.mp.minerMap[cm.connID]; ok {
						sn.recycleTask(cm.connID, mr)
					}
				}
			} else {
				switch cm.mesg.Type {
				case bitcoin.Join:
					sn.cacheMinerMesg(cm)
				case bitcoin.Request:
					sn.handleClientMesg(cm)
				case bitcoin.Result:
					sn.handleMinerResult(cm)
				default:
					sn.logger.Println("Invalid message type: Unknow Message Type!")
				}
			}
		case mr := <-sn.mp.chanIdleMiners:
			sn.assignTask(mr)
		}
	}
}

func createServerNode(port int) *serverNode {
	// only one client request in normal case.
	cp := &clientPool{clientMap: make(map[int]*clientRequest)}
	mp := &minerPool{
		taskListMap:    make(map[int]*list.List),
		chanIdleMiners: make(chan *minerRequest, pool_size),
		minerMap:       make(map[int]*minerRequest),
	}
	sn := &serverNode{
		chanOnExit:     make(chan bool, chan_size_small),
		chanCommonMesg: make(chan *commonMesg, bitcoin.ChanSizeUnit),
		cp:             cp,
		mp:             mp,
	}
	logger, lf, err := bitcoin.BuildLogger()
	if err != nil {
		fmt.Println("Logger build error: ", err)
		return nil
	}
	sn.logger = logger
	sn.lf = lf
	sn.params = bitcoin.MakeParams()
	srv, err := lsp.NewServer(port, sn.params)
	if err != nil {
		sn.logger.Printf("Failed to start server on port %d: %s\n", port, err)
		return nil
	}
	sn.srv = srv
	sn.logger.Printf("serverNode create successfully!.\n")
	return sn
}

func (sn *serverNode) exit() {
	for {
		select {
		case <-sn.chanExit:
			// close server.
			sn.srv.Close()
			close(sn.chanOnExit)
			close(sn.chanExit)
			sn.logger.Printf("Server closed connection and exited.\n")
			// close log file.
			sn.lf.Close()
			return
		}
	}
}
