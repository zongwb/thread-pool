package threadpool

import (
	"sync"
)

// Worker retrieves a JOb from a channel and executes it.
type Worker struct {
	jobChan <-chan Job
	wg      *sync.WaitGroup // MUST be a pointer
	quit    chan bool
}

// NewWorker returns a Worker instance.
func NewWorker(c <-chan Job, wg *sync.WaitGroup) *Worker {
	return &Worker{jobChan: c, wg: wg, quit: make(chan bool, 1)}
}

// Start starts the worker goroutine.
func (w *Worker) Start() {
	go func() {
		for {
			select {
			case job := <-w.jobChan:
				// must process the job synchronously
				job.Process()
			case <-w.quit:
				// signal to the dispatcher
				w.wg.Done()
				return
			}
		}
	}()
}

// Stop stops the worker goroutine.
func (w *Worker) Stop() {
	w.quit <- true
}
