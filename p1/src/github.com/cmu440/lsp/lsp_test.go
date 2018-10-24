package lsp

import (
	"fmt"
	// "math/rand"
	// "container/list"
	"sync"
	"testing"
	"time"
)

// type MesageData struct {
// 	id      int
// 	content string
// }

// func TestMessageWindowOperation(t *testing.T) {
// 	mw := NewMessageWindow(5)
// 	for i := 0; i < 3; i++ {
// 		md := MesageData{id: i, content: fmt.Sprintf("this is content %v", i)}
// 		mw.Put(md)
// 	}
// 	for j := 0; j < 3; j++ {
// 		for item := mw.First(); item != nil; item = mw.Next() {
// 			md := item.(*MesageData)
// 			fmt.Println(md.id, md.content)
// 		}
// 		fmt.Println("==================")
// 	}

// 	fmt.Println(mw.Length())

// 	fmt.Println(mw.index)

// 	mw.ResetIndex()

// 	fmt.Println(mw.index)

// 	for i := 0; i < 3; i++ {
// 		if i%2 != 0 {
// 			md := MesageData{id: i * 10, content: fmt.Sprintf("this is content %v", i*10)}
// 			mw.Set(i, md)
// 		}
// 	}

// 	for item := mw.First(); item != nil; item = mw.Next() {
// 		md := item.(*MesageData)
// 		fmt.Println(md.id, md.content)
// 	}
// 	fmt.Println("==================")

// 	for i := 3; i < 5; i++ {
// 		md := MesageData{id: i, content: fmt.Sprintf("this is content %v", i)}
// 		mw.Put(md)
// 	}

// 	for item := mw.First(); item != nil; item = mw.Next() {
// 		md := item.(*MesageData)
// 		fmt.Println(md.id, md.content)
// 	}
// 	fmt.Println("==================")
// 	for i := 5; i < 10; i++ {
// 		md := MesageData{id: i, content: fmt.Sprintf("this is content %v", i)}
// 		if _, err := mw.Put(md); err != nil {
// 			fmt.Println(err.Error())
// 		}
// 	}
// 	fmt.Println("==================")
// 	for item := mw.First(); item != nil; item = mw.Next() {
// 		md := item.(*MesageData)
// 		fmt.Println(md.id, md.content)
// 	}
// 	fmt.Println("==================")
// 	for item := mw.Last(); item != nil; item = mw.Prev() {
// 		md := item.(*MesageData)
// 		fmt.Println(md.id, md.content)
// 	}
// 	fmt.Println("==================")
// 	for item := mw.First(); item != nil; item = mw.Next() {
// 		md := item.(*MesageData)
// 		fmt.Println(md.id, md.content)
// 	}
// }

// func TestMessageWindowSort(t *testing.T) {
// 	mw := NewMessageWindow(10)
// 	for i := 0; i < 10; i++ {
// 		md := MesageData{id: rand.Intn(100), content: fmt.Sprintf("this is content %v", i)}
// 		mw.Put(md)
// 	}
// 	for j := 0; j < 2; j++ {
// 		for item := mw.First(); item != nil; item = mw.Next() {
// 			md := item.(MesageData)
// 			fmt.Println(md.id, md.content)
// 		}
// 		fmt.Println("==================")
// 	}
// 	mw.bubbleSort()
// 	fmt.Println("排序后")
// 	for item := mw.First(); item != nil; item = mw.Next() {
// 		md := item.(MesageData)
// 		fmt.Println(md.id, md.content)
// 	}

// }

func TestNormalOperation2(t *testing.T) {
	a := 10
	switch a {
	case 1:
		fmt.Println("=")
	case 2:
		fmt.Println("==")
	case 3:
		fmt.Println("===")
	case 4:
		fmt.Println("====")
	default:
		fmt.Println("...")
	}
}

func TestCopy(t *testing.T) {
	s := []int{1, 2, 3}
	fmt.Println(s) //[1 2 3]
	copy(s, []int{4, 5, 6, 7, 8, 9})
	fmt.Println(s) //[4 5 6]
	var d []int
	copy(d, []int{4, 5, 6, 7, 8, 9})
	fmt.Println(d) //[4 5 6]
}

func foo1(d []int) {
	a1 := []int{1, 2, 3, 4, 5}
	for i := 0; i < 5; i++ {
		// d = append(d, a1[i]*2)
		// d[i] = a1[i] * 2
	}
	copy(d, a1)
	for i := 0; i < 5; i++ {
		fmt.Printf("%v ", d[i])
	}
	fmt.Println()
}

func TestArrayArgus(t *testing.T) {
	d := make([]int, 5)
	foo1(d)
	for i := 0; i < 5; i++ {
		fmt.Printf("%v ", d[i])
	}
}

type TestStruct struct {
	id   int
	name string
}

func TestStructRef(t *testing.T) {
	ts1 := &TestStruct{id: 1, name: "ts1"}
	ts2 := &TestStruct{id: 2, name: "ts2"}
	a := [2]*TestStruct{ts1, ts2}
	for i := 0; i < 2; i++ {
		a[i].id = a[i].id * 10
	}
	for i := 0; i < 2; i++ {
		fmt.Printf("%v\n", a[i])
	}
}

func TestChannel1(t *testing.T) {
	ch := make(chan bool, 10)
	for i := 0; i < 5; i++ {
		ch <- true
	}
	fmt.Printf("len(ch)=%v\n", len(ch))
	<-ch
	fmt.Printf("len(ch)=%v\n", len(ch))
	<-ch
	fmt.Printf("len(ch)=%v\n", len(ch))
	<-ch
	<-ch
	fmt.Printf("len(ch)=%v\n", len(ch))
	<-ch
	fmt.Printf("len(ch)=%v\n", len(ch))
	<-ch
}

func foo2(ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("-------------")
	for c := range ch {
		fmt.Printf("len(ch)=%v\n", len(ch))
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("c=%v\n", c)
	}
}

func foo3(ch <-chan int, wg *sync.WaitGroup) {
	fmt.Println("-----------------------------")
	defer wg.Done()
	for true {
		fmt.Printf("========len(ch)=%v\n", len(ch))
		time.Sleep(20 * time.Millisecond)
	}
}
func TestChannel2(t *testing.T) {
	var wg sync.WaitGroup
	ch := make(chan int, 10)
	for i := 0; i < 7; i++ {
		ch <- i
	}
	wg.Add(2)
	// time.Sleep(1000 * time.Millisecond)
	fmt.Printf("^^^^^^^^^^^^^len(ch)=%v\n", len(ch))
	go foo2(ch, &wg)
	go foo3(ch, &wg)
	wg.Wait()
}

func TestMapConcurrentUpdate(t *testing.T) {
	var m1 map[string]string
	m1 = make(map[string]string)
	m1["a"] = "aa"
	m1["b"] = "bb"
	m1["c"] = "cc"
	fmt.Println(m1)

	for k, v := range m1 {
		fmt.Println(k, v)
		delete(m1, k)
	}

	fmt.Println(m1)
}

func TestAssign(t *testing.T) {
	m1 := make(map[string]string)
	m1["a"] = "aa"
	m1["b"] = "bb"
	m1["c"] = "cc"

	m2 := m1
	delete(m1, "a")
	delete(m1, "b")

	fmt.Println(m2)
}
