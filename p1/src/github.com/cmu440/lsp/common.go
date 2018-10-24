package lsp

import (
	"sync/atomic"
)

var nextClientId int32 = 0
var nextConnId int32 = 0

func getNextClientId() int {
	return int(atomic.AddInt32(&nextClientId, 1))
}

func getNextConnId() int {
	return int(atomic.AddInt32(&nextConnId, 1))
}
