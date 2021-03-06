package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"sync"
)

type minerNode struct {
	mnID            int
	cli             lsp.Client
	mu              sync.Mutex
	logger          *log.Logger
	lf              *os.File
	chanOnExit      chan bool
	chanExit        chan bool
	params          *lsp.Params
	chanRequestMesg chan *bitcoin.Message
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	mn := createMinerNode(os.Args[1])
	mn.sendServerJoin()
	go mn.readMesg()
	go mn.handleStuff()
	mn.exit()
}

func (mn *minerNode) sendServerJoin() {
	join := bitcoin.NewJoin()
	mdBytes, _ := json.Marshal(join)
	mn.cli.Write(mdBytes)
}

func (mn *minerNode) sendServerResult(mesg *bitcoin.Message) {
	mdBytes, _ := json.Marshal(mesg)
	mn.cli.Write(mdBytes)
	mn.logger.Printf("Miner(%v) send message(%v) to server successfully.\n", mn.mnID, mesg.String())
}

func (mn *minerNode) handleMesg(mesg *bitcoin.Message) *bitcoin.Message {
	mn.logger.Printf("Miner(%v) begin to handle message(%v).\n", mn.mnID, mesg.String())
	resultHash := ^uint64(0) - 1 // max uint64 - 1
	resultNonce := uint64(1)
	for i := mesg.Lower; i < mesg.Upper; i++ {
		hv := bitcoin.Hash(mesg.Data, i)
		if hv < resultHash {
			resultHash = hv
			resultNonce = i
		}
	}
	rsltMesg := bitcoin.NewResult(resultHash, resultNonce)
	mn.logger.Printf("Miner(%v) finished handling message(%v), result is message(%v).\n", mn.mnID, mesg.String(), rsltMesg.String())
	return rsltMesg
}

func (mn *minerNode) readMesg() {
	defer mn.logger.Printf("[readMesg] Miner(%v) read exiting.\n", mn.mnID)
	for {
		data, err := mn.cli.Read()
		if err != nil {
			mn.logger.Println("Miner received error during read.")
			mn.chanOnExit <- true
			return
		}
		var mesg bitcoin.Message
		json.Unmarshal(data, &mesg)
		mn.logger.Printf("Miner read message %s from server.\n", mesg.String())
		if mesg.Type != bitcoin.Request {
			mn.logger.Printf("Unsupported message type: %v!", bitcoin.Request)
		} else {
			mn.chanRequestMesg <- &mesg
		}
	}
}

func (mn *minerNode) handleStuff() {
	defer mn.logger.Printf("[handleStuff] Miner(%v) handle message is exiting.\n", mn.mnID)
	for {
		select {
		case <-mn.chanOnExit:
			mn.chanExit <- true
			return
		case mesg := <-mn.chanRequestMesg:
			resultMesg := mn.handleMesg(mesg)
			mn.sendServerResult(resultMesg)
		}
	}
}

func createMinerNode(hostport string) *minerNode {
	mn := &minerNode{mnID: bitcoin.GetNextMinerId()}
	logger, lf, err := bitcoin.BuildLogger()
	if err != nil {
		fmt.Println("Logger build error: ", err)
		return nil
	}
	mn.logger = logger
	mn.lf = lf
	mn.params = bitcoin.MakeParams()
	cli, err := lsp.NewClient(hostport, mn.params)
	if err != nil {
		mn.logger.Printf("Client failed to connect to server(%v): %s.", hostport, err)
		return nil
	}
	mn.cli = cli
	mn.chanRequestMesg = make(chan *bitcoin.Message, bitcoin.ChanSizeUnit)
	mn.chanOnExit = make(chan bool, bitcoin.ChanSizeUnit)
	mn.chanExit = make(chan bool, bitcoin.ChanSizeUnit)
	mn.logger.Println("Miner node created.")
	return mn
}

func (mn *minerNode) exit() {
	for {
		select {
		case <-mn.chanExit:
			// close client connection.
			mn.cli.Close()
			close(mn.chanOnExit)
			close(mn.chanExit)
			mn.logger.Printf("Miner(%v) closed connection and exited.\n", mn.mnID)
			// close log file.
			mn.lf.Close()
			return
		}
	}

}
