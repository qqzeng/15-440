package lsp

import (
	"errors"
	"fmt"
)

// type SWMessage struct {
// 	ack bool
// 	msg interface{}
// }

// Non-thread safety!
type MessageWindow struct {
	size     int        // message window size
	capacity int        // window capacity.
	index    int        // current index pointer.
	mesgs    []*Message // messages array.
}

func NewMessageWindow(capacity int) *MessageWindow {
	mes := &MessageWindow{
		size:     0,
		capacity: capacity,
		index:    -1,
	}
	return mes
}

func (mw *MessageWindow) Length() int {
	return mw.size
}

func (mw *MessageWindow) Next() *Message {
	if mw.checkSize() != nil {
		return nil
	}
	if mw.index+1 < mw.size {
		mw.index++
		return mw.mesgs[mw.index]
	}
	return nil
}

func (mw *MessageWindow) Prev() *Message {
	if mw.checkSize() != nil {
		return nil
	}
	if mw.index-1 >= 0 {
		mw.index--
		return mw.mesgs[mw.index]
	}
	return nil
}

func (mw *MessageWindow) First() *Message {
	if mw.checkSize() != nil {
		return nil
	}
	mw.index = 0
	return mw.mesgs[mw.index]
}

func (mw *MessageWindow) Last() *Message {
	if mw.checkSize() != nil {
		return nil
	}
	mw.index = mw.size - 1
	return mw.mesgs[mw.index]
}

func (mw *MessageWindow) ResetIndex() error {
	if mw.checkSize() != nil {
		return nil
	}
	mw.index = 0
	return nil
}

func (mw *MessageWindow) Put(value *Message) (*Message, error) {
	if mw.size == mw.capacity {
		fmt.Println("MessageWindow Window size reaches its maximmum capacity.")
		for i := 1; i < mw.size; i++ {
			mw.mesgs[i-1] = mw.mesgs[i]
		}
		mw.index = mw.size - 1
		mw.Set(mw.size-1, value)
	} else {
		mw.index++
		mw.mesgs = append(mw.mesgs, value)
		mw.size++
	}
	// sort ascending.
	mw.bubbleSort()
	return value, nil
}

func (mw *MessageWindow) Set(idx int, value *Message) (*Message, error) {
	if idx < 0 || idx > mw.size-1 {
		return nil, errors.New("Error setting index.")
	}
	mw.mesgs[idx] = value
	return mw.mesgs[idx], nil
}

func (mw *MessageWindow) checkSize() error {
	if mw.size == 0 {
		return errors.New("Window size is zero.")
	}
	return nil
}

func (mw *MessageWindow) bubbleSort() {
	for i := 0; i < mw.Length()-1; i++ {
		for j := 0; j < mw.Length()-1-i; j++ {
			mg1 := mw.mesgs[j]
			mg2 := mw.mesgs[j+1]
			if mg1.SeqNum > mg2.SeqNum {
				mw.mesgs[j+1], mw.mesgs[j] = mw.mesgs[j], mw.mesgs[j+1]
			}
		}
	}
}

// func (mw *MessageWindow) bubbleSort() {
// 	for i := 0; i < mw.Length()-1; i++ {
// 		for j := 0; j < mw.Length()-1-i; j++ {
// 			mg1 := mw.mesgs[j].(MesageData)
// 			mg2 := mw.mesgs[j+1].(MesageData)
// 			if mg1.id > mg2.id {
// 				mw.mesgs[j+1], mw.mesgs[j] = mw.mesgs[j], mw.mesgs[j+1]
// 			}
// 		}
// 	}
// }

// type Messages []Message

// // implementation methods for sort.//
// func (ms Messages) Len() int { return len(ms) }

// // accesding.
// func (ms Messages) Less(i, j int) bool {
// 	return ms[i].SeqNum < ms[j].SeqNum
// }

// func (ms Messages) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
