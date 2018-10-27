package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	CHAN_SIZE_UNIT = 1
)

type clientNode struct {
	cnID     int
	cli      lsp.Client
	mu       sync.Mutex
	logger   *log.Logger
	lf       *os.File
	chanExit chan bool
	params   *lsp.Params
}

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}
	upper, err := strconv.ParseUint(os.Args[3], 10, 32)
	if err != nil {
		fmt.Println("maxNonce error!")
		return
	}
	cn, err := createClientNode(os.Args[1])
	if err != nil {
		return
	}
	if err := cn.sendServerRequest(os.Args[2], upper); err != nil {
		return
	}
	cn.handleStuff()
	go cn.exit()
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}

func (cn *clientNode) handleMesg(mesg *bitcoin.Message) {
	hash := strconv.FormatUint(mesg.Hash, 10)
	nonce := strconv.FormatUint(mesg.Nonce, 10)
	printResult(hash, nonce)
}

func (cn *clientNode) sendServerRequest(data string, maxNonce uint64) error {
	mesg := bitcoin.NewRequest(data, 0, maxNonce)
	mdBytes, _ := json.Marshal(mesg)
	err := cn.cli.Write(mdBytes)
	if err != nil {
		printDisconnected()
		cn.chanExit <- true
		return err
	}
	cn.logger.Printf("Client(%v) send message(%v) to server successfully.\n", cn.cnID, mesg.String())
	return nil
}

func (cn *clientNode) handleStuff() {
	defer cn.logger.Printf("Client(%v) is exiting.\n", cn.cnID)
	for {
		data, err := cn.cli.Read()
		if err != nil {
			printDisconnected()
			cn.chanExit <- true
			cn.logger.Println("Client received error during read.")
			return
		}
		var mesg bitcoin.Message
		json.Unmarshal(data, &mesg)
		cn.logger.Printf("Client read message %s from server.\n", mesg.String())
		if mesg.Type != bitcoin.Result {
			cn.logger.Printf("Unsupported message type: %v!", bitcoin.Request)
		} else {
			cn.handleMesg(&mesg)
		}
	}
}

func createClientNode(hostport string) (*clientNode, error) {
	cn := &clientNode{cnID: bitcoin.GetNextClientId()}
	logger, lf, err := bitcoin.BuildLogger()
	if err != nil {
		fmt.Println("Logger build error: ", err)
		return nil, err
	}
	cn.logger = logger
	cn.lf = lf
	cn.params = bitcoin.MakeParams()
	cli, err := lsp.NewClient(hostport, cn.params)
	if err != nil {
		cn.logger.Printf("Client failed to connect to server(%v): %s.", hostport, err)
		printDisconnected()
		cn.chanExit <- true
		return nil, err
	}
	cn.cli = cli
	cn.chanExit = make(chan bool, CHAN_SIZE_UNIT)
	cn.logger.Println("Miner node created.")
	return cn, nil
}

func (cn *clientNode) exit() {
	for {
		select {
		case <-cn.chanExit:
			// close client connection.
			cn.cli.Close()
			close(cn.chanExit)
			cn.logger.Printf("Miner(%v) closed connection and exited.\n", cn.cnID)
			// close log file.
			cn.lf.Close()
			return
		}
	}
}
