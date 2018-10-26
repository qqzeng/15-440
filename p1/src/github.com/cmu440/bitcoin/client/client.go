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

type clientNode struct {
	cnID     int
	cli      lsp.Client
	mu       sync.Mutex
	logger   *log.Logger
	chanExit chan bool
	params   *lsp.Params
}

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}
	maxNonce, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("maxNonce error!")
		return
	}
	cn := createClientNode(os.Args[1])
	cn.sendServerRequest(os.Args[2], uint64(maxNonce))
	go cn.handleStuff()
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
	printResult(string(mesg.Hash), string(mesg.Nonce))
}

func (cn *clientNode) sendServerRequest(data string, maxNonce uint64) {
	mesg := bitcoin.NewRequest(data, 0, maxNonce)
	mdBytes, _ := json.Marshal(mesg)
	if err := cn.cli.Write(mdBytes); err != nil {
		cn.chanExit <- true
		printDisconnected()
		cn.logger.Printf("Client(%v) fails to send message(%v) to server, error: %v.\n", cn.cnID, mesg.String(), err.Error())
		return
	}
	cn.logger.Printf("Client(%v) send message(%v) to server successfully.\n", cn.cnID, mesg.String())
}

func (cn *clientNode) handleStuff() {
	defer cn.logger.Printf("Client(%v) exiting.\n", cn.cnID)
	for {
		select {
		case <-cn.chanExit:
			return
		default:
			data, err := cn.cli.Read()
			if err != nil {
				cn.logger.Println("Client received error during read.")
				return
			}
			var mesg *bitcoin.Message
			json.Unmarshal(data, mesg)
			cn.logger.Printf("Client read message %s from server.\n", mesg.String())
			if mesg.Type != bitcoin.Result {
				cn.logger.Printf("Unsupported message type: %v!", bitcoin.Request)
			} else {
				cn.handleMesg(mesg)
			}
		}
	}
}

func createClientNode(hostport string) *clientNode {
	cn := &clientNode{cnID: bitcoin.GetNextClientId()}
	logger, err := bitcoin.BuildLogger()
	if err != nil {
		fmt.Println("Logger build error: ", err)
		return nil
	}
	cn.logger = logger
	cn.params = bitcoin.MakeParams()
	cli, err := lsp.NewClient(hostport, cn.params)
	if err != nil {
		cn.logger.Printf("Client failed to connect to server(%v): %s.", hostport, err)
		return nil
	}
	cn.cli = cli
	cn.logger.Println("Miner node created.")
	return cn
}
