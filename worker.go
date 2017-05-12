package threadpool

import (
	"log"
	"sync"
)

type Worker struct {
	jobChan <-chan Job
	wg      *sync.WaitGroup // MUST be a pointer
	quit    chan bool
}

func NewWorker(c <-chan Job, wg *sync.WaitGroup) *Worker {
	return &Worker{jobChan: c, wg: wg, quit: make(chan bool, 1)}
}

func (w *Worker) Start() {
	go func() {
		done := false
		for {
			if done {
				return
			}
			// Enclose processing in a function so as to recover from panic
			func() {
				defer func() {
					err := recover()
					if err != nil {
						log.Println("Error in worker thread, recovered from ", err)
					}
				}()

				select {
				case job := <-w.jobChan:
					// must process the job synchronously
					job.Process()
				case <-w.quit:
					// signal to the dispatcher
					w.wg.Done()
					// signal the goroutine
					done = true
					return
				}
			}()
		}
	}()
}

func (w *Worker) Stop() {
	w.quit <- true
}
