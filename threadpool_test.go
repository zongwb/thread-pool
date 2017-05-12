package threadpool

import (
	"fmt"
	"os"
	"testing"
	"time"

	tb "git.garena.com/gocommon/TokenBucket"
)

func TestMain(m *testing.M) {
	fmt.Println("testing threadpool\n")

	os.Exit(m.Run())
}

var pool ThreadPool

func producerA() {
	for {
		j := &job{done: make(chan bool, 1)}
		err := pool.BPost(j)
		if err != nil {
			fmt.Printf("producerA failed to post, %v\n", err)
			time.Sleep(time.Second)
			continue
		} else {
			fmt.Printf("producerA post 1\n")
		}
		select {
		case <-j.done:
		case <-time.After(time.Millisecond * 200):
			j.cancelled = true
		}
	}
}
func prodducerB() {
	for {
		j := &job{done: make(chan bool, 1)}
		err := pool.Post(j)
		if err != nil {
			fmt.Printf("prodducerB failed to post, %v\n", err)
			time.Sleep(time.Second)
			continue
		} else {
			fmt.Printf("prodducerB post 1\n")
		}
		select {
		case <-j.done:
		case <-time.After(time.Millisecond * 200):
			j.cancelled = true
		}
	}
}

func stop() {
	time.Sleep(5 * time.Second)
	for {
		err := pool.Stop()
		if err != nil {
			fmt.Printf("stop1 failed, %v\n", err)
			time.Sleep(time.Second)
		} else {
			fmt.Printf("stop succeeded\n")
			break
		}
	}
}

func bstop() {
	time.Sleep(5 * time.Second)
	for {
		err := pool.BStop()
		if err != nil {
			fmt.Printf("bstop failed, %v\n", err)
			time.Sleep(time.Second)
		} else {
			fmt.Printf("bstop succeeded\n")
			break
		}
	}
}

type job struct {
	cancelled bool
	done      chan bool
}

func (j *job) Cancelled() bool {
	return j.cancelled
}

func (j *job) Process() error {
	fmt.Printf("processing job...\n")
	time.Sleep(time.Second)
	j.done <- true
	return nil
}
func (j *job) String() string {
	return "Dummy job"
}

func TestPush(t *testing.T) {
	token := tb.NewTokenBucket(10)
	pool = NewThreadPool(3, 5, token)
	pool.Run()
	go producerA()
	go prodducerB()
	go stop()
	go bstop()
	time.Sleep(10 * time.Second)
	pool.Run()
	time.Sleep(5 * time.Second)
}
