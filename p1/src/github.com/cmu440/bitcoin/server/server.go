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
	"sync"
)

// client reqeust struct
type cliRequest struct {
	connID int
	mesg   *bitcoin.Message
}

type JobStatus int

const (
	Completed JobStatus = iota
	Running
	Abort
)

type job struct {
	jobId    int
	mesg     *bitcoin.Message
	rsltMesg *bitcoin.Message
	status   JobStatus
}

type work struct {
	jobId  int
	jobNum int
	connID int // client connection id
}

type minerRequest struct {
	connID int
	mesg   *bitcoin.Message
	jobs   *list.List
}

type serverNode struct {
	mu              sync.Mutex
	srv             lsp.Server
	chanExit        chan bool
	chanCliRequest  chan *cliRequest
	minerRequestMap map[int]*minerRequest // minner join request map
	workList        *list.List
	params          *lsp.Params
	logger          *log.Logger
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
	go sn.handleStuff()
	go sn.distributeRequests()
}

func (sn *serverNode) sendMinerRequest() {
	sn.logger.Printf("server begin to send message to miners.\n")
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for connID, mr := range sn.minerRequestMap {
		mesg := mr.jobs.Front().Value.(*job).mesg
		mdBytes, _ := json.Marshal(mesg)
		sn.srv.Write(connID, mdBytes)
		// TODO: handle miner failures
		sn.logger.Printf("server finishes sending message(%v) to miner(%v).\n", mesg.String(), connID)
	}
	sn.logger.Printf("server finishes send message to miners.\n")
}

func (sn *serverNode) sendClientResult(connID int, mesg *bitcoin.Message) {
	sn.logger.Printf("server begin to send message to miners.\n")
	mdBytes, _ := json.Marshal(mesg)
	// TODO: handle client failures, e.g. connection lost
	if err := sn.srv.Write(connID, mdBytes); err != nil {
		sn.logger.Printf("server abort sending result(%v) to client(%v) due to connection lost.\n", mesg.String(), connID)
	}
}

func (sn *serverNode) distributeRequests() {
	for {
		select {
		// Process one request at a time
		case cliReq := <-sn.chanCliRequest:
			sn.mu.Lock()
			start := cliReq.mesg.Lower
			// assign the request to all live minners. no load balance stragety.
			avg := (cliReq.mesg.Upper - cliReq.mesg.Lower) / uint64(len(sn.minerRequestMap))
			jobId := bitcoin.GetNextJobId()
			w := &work{jobId: jobId, jobNum: len(sn.minerRequestMap), connID: cliReq.connID}
			sn.workList.PushBack(w)
			i := 1
			end := start + avg
			for _, mr := range sn.minerRequestMap {
				if i == len(sn.minerRequestMap) {
					end = cliReq.mesg.Upper + 1 // including upper number
				} else {
					end = start + avg
				}
				// sub-message reqeust handled by per miner
				msg := bitcoin.NewRequest(cliReq.mesg.Data, start, end)
				job := &job{
					jobId:  jobId,
					mesg:   msg,
					status: Running,
				}
				mr.jobs.PushBack(job)
				start = end
				i++
			}
			sn.mu.Unlock()
			sn.sendMinerRequest()
		}
	}
}

func (sn *serverNode) updateMinerMesg(connID int, mesg *bitcoin.Message) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	jobs := sn.minerRequestMap[connID].jobs
	j := jobs.Front().Value.(*job)
	j.status = Completed
	j.rsltMesg = mesg
	successJobNum := 0
	e := sn.workList.Front()
	if e == nil {
		return
	}
	w := e.Value.(*work)
	resultHash := ^uint64(0) - 1 // max uint - 1 (1<<64 - 1)
	resultNonce := uint64(1)
	for _, mr := range sn.minerRequestMap {
		if e := mr.jobs.Front(); e != nil {
			j := (e.Value).(*job)
			if j.status == Completed && w.jobId == j.jobId {
				successJobNum += 1
				if j.rsltMesg.Hash < resultHash { // find the least hash.
					resultHash = j.rsltMesg.Hash
					resultNonce = j.rsltMesg.Nonce
				}
			}
		}
	}
	if successJobNum == w.jobNum {
		sn.sendClientResult(w.connID, bitcoin.NewResult(resultHash, resultNonce))
	}
}

func (sn *serverNode) cacheMinerMesg(connID int, mesg *bitcoin.Message) {
	sn.mu.Lock()
	minerReq := &minerRequest{
		connID: connID,
		mesg:   mesg,
		jobs:   list.New(), // initial empty job list.
	}
	sn.logger.Printf("Server cached Join message %s from minner(%d).\n", mesg.String(), connID)
	sn.minerRequestMap[connID] = minerReq
	sn.mu.Unlock()
}

func (sn *serverNode) cacheCliMesg(connID int, mesg *bitcoin.Message) {
	cliReq := &cliRequest{
		connID: connID,
		mesg:   mesg,
	}
	sn.chanCliRequest <- cliReq
	sn.logger.Printf("Server cached Client message %s from client(%d).\n", mesg.String(), connID)
}

func (sn *serverNode) handleStuff() {
	defer sn.logger.Printf("Server exiting.\n")
	for {
		select {
		case <-sn.chanExit:
			return
		default:
			connID, data, err := sn.srv.Read()
			if err != nil {
				sn.logger.Println("Server received error during read.")
				return
			}
			// mesg, err := data.(*Message)
			var mesg *bitcoin.Message
			json.Unmarshal(data, mesg)
			sn.logger.Printf("Server read message %s from client %d.\n", mesg.String(), connID)
			switch mesg.Type {
			case bitcoin.Join:
				go sn.cacheMinerMesg(connID, mesg)
			case bitcoin.Request:
				go sn.cacheCliMesg(connID, mesg)
			case bitcoin.Result:
				go sn.updateMinerMesg(connID, mesg)
			default:
				sn.logger.Println("Invalid message type: Unknow Message Type!")
			}
		}
	}
}

func createServerNode(port int) *serverNode {
	sn := &serverNode{
		minerRequestMap: make(map[int]*minerRequest),
		workList:        list.New(),
		chanCliRequest:  make(chan *cliRequest, 1),
	}
	logger, err := bitcoin.BuildLogger()
	if err != nil {
		fmt.Println("Logger build error: ", err)
		return nil
	}
	sn.logger = logger
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
