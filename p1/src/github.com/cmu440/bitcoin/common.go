package bitcoin

import (
	"github.com/cmu440/lsp"
	"log"
	"os"
	"sync/atomic"
)

const (
	name = "log-server.txt"
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)

	// default global params
	defaultEpochLimit  = 5
	defaultEpochMillis = 1000
	defaultWindowSize  = 3
)

func BuildLogger() (*log.Logger, *os.File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, nil, err
	}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	return LOGF, file, nil
}

func MakeParams() *lsp.Params {
	return &lsp.Params{
		EpochLimit:  defaultEpochLimit,
		EpochMillis: defaultEpochMillis,
		WindowSize:  defaultWindowSize,
	}
}

var nextJobId int32 = 0

func GetNextJobId() int {
	return int(atomic.AddInt32(&nextJobId, 1))
}

var nextMinerNodeId int32 = 0

func GetNextMinerId() int {
	return int(atomic.AddInt32(&nextMinerNodeId, 1))
}

var nextClientNodeId int32 = 0

func GetNextClientId() int {
	return int(atomic.AddInt32(&nextClientNodeId, 1))
}
