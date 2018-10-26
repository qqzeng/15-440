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
	"sync/atomic"
)

const (
	name = "log.txt"
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)

	// default global params
	defaultEpochLimit  = 5
	defaultEpochMillis = 1000
	defaultWindowSize  = 3
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
	jobId  int
	mesg   *bitcoin.Message
	status JobStatus
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

	// TODO: implement this!
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port error!")
		return
	}
	sn := createServerNode(port)
	sn.logger.Printf("serverNode create successfully!.\n")

	go sn.handleMesg()
	go sn.distributeRequests()
}

func (sn *serverNode) sendMinerRequest() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for connID, mr := range sn.minerRequestMap {
		mesg := mr.jobs.Front().Value.(*job).mesg
		mdBytes, _ := json.Marshal(mesg)
		sn.srv.Write(connID, mdBytes)
	}
}

func (sn *serverNode) sendClientResult(connID int, mesg *bitcoin.Message) {
	mdBytes, _ := json.Marshal(mesg)
	sn.srv.Write(connID, mdBytes)
}

func (sn *serverNode) distributeRequests() {
	for {
		select {
		case cliReq := <-sn.chanCliRequest:
			sn.mu.Lock()
			start := cliReq.mesg.Lower
			// assign the request to all live minners.
			avg := (cliReq.mesg.Upper - cliReq.mesg.Lower) / uint64(len(sn.minerRequestMap))
			jobId := getNextJobId()
			w := &work{jobId: jobId, jobNum: len(sn.minerRequestMap), connID: cliReq.connID}
			sn.workList.PushBack(w)
			for _, mr := range sn.minerRequestMap {
				// sub-message reqeust handled by per miner
				msg := bitcoin.NewRequest(cliReq.mesg.Data, start, start+avg)
				// job info for every sub-message matained for miner
				job := &job{
					jobId:  jobId,
					mesg:   msg,
					status: Running,
				}
				mr.jobs.PushBack(job)
				start = start + avg + 1
			}
			sn.mu.Unlock()
			sn.sendMinerRequest()
		}
	}
}

func (sn *serverNode) updateMinerMesg(connID int, data *bitcoin.Message) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.minerRequestMap[connID].jobs.Front().Value.(*job).status = Completed
	successJobNum := 0
	e := sn.workList.Front()
	if e == nil {
		return
	}
	w := e.Value.(*work)
	resultHash := ^uint64(0) - 1 // max uint - 1
	resultNonce := uint64(1)
	for _, mr := range sn.minerRequestMap {
		if e := mr.jobs.Front(); e != nil {
			j := e.Value.(*job)
			if j.status == Completed && w.jobId == j.jobId {
				successJobNum += 1
				if j.mesg.Hash < resultHash { // find the least hash.
					resultHash = j.mesg.Hash
					resultNonce = j.mesg.Nonce
				}
			}
		}
	}
	if successJobNum == w.jobNum {
		sn.sendClientResult(w.connID, bitcoin.NewResult(resultHash, resultNonce))
	}
}

func (sn *serverNode) cacheMinerMesg(connID int, data *bitcoin.Message) {
	// wait for goroutine read the minerRequestList.
	sn.mu.Lock()
	minerReq := &minerRequest{
		connID: connID,
		mesg:   data,
		jobs:   list.New(), // initial empty job map.
	}
	sn.logger.Printf("Server cached Join message %s from minner %d.\n", data.String(), connID)
	sn.minerRequestMap[connID] = minerReq
	sn.mu.Unlock()
}

func (sn *serverNode) cacheCliMesg(connID int, data *bitcoin.Message) {
	// wait for goroutine read the cliRequestList.
	cliReq := &cliRequest{
		connID: connID,
		mesg:   data,
	}
	sn.chanCliRequest <- cliReq
	sn.logger.Printf("Server cached Client message %s from client %d.\n", data.String(), connID)
}

func (sn *serverNode) handleMesg() {
	defer sn.logger.Println("Server shutting down...")
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
				sn.logger.Println("Invalid message type!")
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
	logger, err := buildLogger()
	if err != nil {
		fmt.Println("Logger build error: ", err)
		return nil
	}
	sn.logger = logger
	sn.params = makeParams()
	srv, err := lsp.NewServer(port, sn.params)
	if err != nil {
		sn.logger.Printf("Failed to start server on port %d: %s\n", port, err)
		return nil
	}
	sn.srv = srv
	return sn
}

func buildLogger() (*log.Logger, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	return LOGF, nil
}

func makeParams() *lsp.Params {
	return &lsp.Params{
		EpochLimit:  defaultEpochLimit,
		EpochMillis: defaultEpochMillis,
		WindowSize:  defaultWindowSize,
	}
}

var nextJobId int32 = 0

func getNextJobId() int {
	return int(atomic.AddInt32(&nextJobId, 1))
}
