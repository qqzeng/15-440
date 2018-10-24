package lsp

import (
	"errors"
	"fmt"
)

type SWMessage struct {
	ack bool
	msg *Message
}

// Non-thread safety!
type MessageWindowSent struct {
	size     int          // message window size
	capacity int          // window capacity.
	index    int          // current index pointer.
	mesgs    []*SWMessage // messages array.
}

func NewMessageWindowSent(capacity int) *MessageWindowSent {
	mws := &MessageWindowSent{
		size:     0,
		capacity: capacity,
		index:    -1,
	}
	return mws
}

func (mws *MessageWindowSent) Length() int {
	return mws.size
}

func (mws *MessageWindowSent) Next() *SWMessage {
	if mws.checkSize() != nil {
		return nil
	}
	if mws.index+1 < mws.size {
		mws.index++
		return mws.mesgs[mws.index]
	}
	return nil
}

func (mws *MessageWindowSent) Prev() *SWMessage {
	if mws.checkSize() != nil {
		return nil
	}
	if mws.index-1 >= 0 {
		mws.index--
		return mws.mesgs[mws.index]
	}
	return nil
}

func (mws *MessageWindowSent) First() *SWMessage {
	if mws.checkSize() != nil {
		return nil
	}
	mws.index = 0
	return mws.mesgs[mws.index]
}

func (mws *MessageWindowSent) Last() *SWMessage {
	if mws.checkSize() != nil {
		return nil
	}
	mws.index = mws.size - 1
	return mws.mesgs[mws.index]
}

func (mws *MessageWindowSent) ResetIndex() error {
	if mws.checkSize() != nil {
		return nil
	}
	mws.index = 0
	return nil
}

func (mws *MessageWindowSent) Put(value *SWMessage) (*SWMessage, error) {
	if mws.size == mws.capacity {
		fmt.Println("MessageWindowSent Window size reaches its maximmum capacity.")
		if mws.mesgs[0].ack == false {
			fmt.Printf("ERROR: %v should not be put into MessageWindowSent sliding window.\n", value.msg.String())
		}
		for i := 1; i < mws.size; i++ {
			mws.mesgs[i-1] = mws.mesgs[i]
		}
		mws.index = mws.size - 1
		mws.Set(mws.size-1, value)
	} else {
		mws.index++
		mws.mesgs = append(mws.mesgs, value)
		mws.size++
	}
	// sort ascending.
	mws.bubbleSort()
	return value, nil
}

func (mws *MessageWindowSent) Set(idx int, value *SWMessage) (*SWMessage, error) {
	if idx < 0 || idx > mws.size-1 {
		return nil, errors.New("Error setting index.")
	}
	mws.mesgs[idx] = value
	return mws.mesgs[idx], nil
}

func (mws *MessageWindowSent) checkSize() error {
	if mws.size == 0 {
		return errors.New("Window size is zero.")
	}
	return nil
}

func (mws *MessageWindowSent) bubbleSort() {
	for i := 0; i < mws.Length()-1; i++ {
		for j := 0; j < mws.Length()-1-i; j++ {
			mg1 := mws.mesgs[j].msg
			mg2 := mws.mesgs[j+1].msg
			if mg1.SeqNum > mg2.SeqNum {
				mws.mesgs[j+1], mws.mesgs[j] = mws.mesgs[j], mws.mesgs[j+1]
			}
		}
	}
}

// type Messages []Message

// // implementation methods for sort.//
// func (ms Messages) Len() int { return len(ms) }

// // accesding.
// func (ms Messages) Less(i, j int) bool {
// 	return ms[i].SeqNum < ms[j].SeqNum
// }

// func (ms Messages) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
