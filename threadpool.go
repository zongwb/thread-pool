package threadpool

import (
	"errors"
	"log"
	"sync"

	tb "git.garena.com/gocommon/TokenBucket"
)

const (
	jobQueueSize = 100
)

type stateType int

const (
	_stopped stateType = iota
	_running
	_stopping
)

type Job interface {
	Process() (err error)
	String() string
}

type CancellableJob interface {
	Job
	Cancelled() bool
}

type ThreadPool interface {
	Run() error
	Stop() error
	//Wait() error
	BStop() error
	Post(job Job) error
	BPost(job Job) error
	PendingJobs() int
}

func NewThreadPool(maxworkers uint32, queuesize uint32, limiter tb.RateLimiter) ThreadPool {
	return newDispatcher(maxworkers, queuesize, limiter)
}

type dispatcher struct {
	sync.Mutex
	jobQueue   chan Job // from the task driver
	jobChan    chan Job // to the workers
	workers    chan *Worker
	queuesize  uint32
	maxworkers uint32
	wg         sync.WaitGroup
	status     stateType
	limiter    tb.RateLimiter
}

// newDispatcher creates a thread-safe dispatcher which implements
// the ThreadPool interface.
func newDispatcher(maxworkers uint32, queuesize uint32, limiter tb.RateLimiter) *dispatcher {
	if maxworkers < 1 {
		maxworkers = 1
	}
	if queuesize < 1 {
		queuesize = 1
	}

	d := &dispatcher{queuesize: queuesize, maxworkers: maxworkers, status: _stopped, limiter: limiter}
	return d
}

// Run should only be called once in a dispatcher's lifetime.
// A job is checked for 'cancelled' status before consuming a token.
func (d *dispatcher) Run() error {
	if d == nil {
		return errors.New("no threadpool instance")
	}
	// Use the double-checked locking pattern
	if d.status != _stopped {
		return errors.New("threadpool is already running or being shut down")
	}
	d.Lock()
	defer d.Unlock()
	if d.status != _stopped {
		return errors.New("threadpool is already running or being shut down")
	}
	// start workers
	d.jobChan = make(chan Job)
	d.jobQueue = make(chan Job, d.queuesize)
	d.workers = make(chan *Worker, d.maxworkers)
	for i := uint32(0); i < d.maxworkers; i++ {
		w := NewWorker(d.jobChan, &d.wg)
		d.workers <- w
		w.Start()
		d.wg.Add(1)
	}
	go d.dispatch()
	d.status = _running
	return nil
}

func (d *dispatcher) dispatch() {
loop:
	for {
		// Rate limitting if so configured
		if d.limiter != nil {
			_ = d.limiter.GetToken(0)
		}
		// In this for loop, we only get process one job
		for job := range d.jobQueue {
			// Skip if cancelled
			if j, ok := job.(CancellableJob); ok {
				if j.Cancelled() {
					log.Printf("job canncelled in dispatcher, %v", job)
					continue
				}
			}
			d.jobChan <- job
			goto loop
		}
		// No more job, we are done
		break
	}
	close(d.workers)
	for w := range d.workers {
		w.Stop()
	}
}

func (d *dispatcher) stop() {
	close(d.jobQueue)
	d.status = _stopping
}

func (d *dispatcher) wait() {
	d.wg.Wait()
	d.status = _stopped
}

// Stop signals the threadpool to stop, but pending jobs will continue to execute.
// Client should call BStop() insead if it is important to wait for jobs in the queue to complete.
func (d *dispatcher) Stop() error {
	if d == nil {
		return errors.New("no threadpool instance")
	}
	// Double-checked locking
	if d.status != _running {
		return nil
	}
	d.Lock()
	defer d.Unlock()
	if d.status != _running {
		return nil
	}
	d.stop()
	return nil
}

// BStop stops the threadpool and waits till all pending jobs are finished.
func (d *dispatcher) BStop() error {
	if d == nil {
		return errors.New("no threadpool instance")
	}
	// Double-checked locking
	if d.status == _stopped {
		return nil
	}
	d.Lock()
	defer d.Unlock()
	if d.status == _stopped {
		return nil
	}
	if d.status == _running {
		d.stop()
	}
	if d.status == _stopping {
		d.wait()
	}
	return nil
}

func (d *dispatcher) Wait() error {
	if d == nil {
		return errors.New("Empty dispatcher")
	}
	// Double-checked locking
	if d.status == _stopped {
		return nil
	}
	if d.status == _running {
		return errors.New("threadpool still running, call stop() first")
	}
	d.Lock()
	defer d.Unlock()
	if d.status == _stopped {
		return nil
	}
	if d.status == _running {
		return errors.New("threadpool still running, call stop() first")
	}
	d.wait()
	return nil
}

// Post sends a job to the threadpool and block if queue is full.
func (d *dispatcher) BPost(job Job) error {
	if d == nil {
		return errors.New("Empty dispatcher")
	}
	if d.status != _running {
		return errors.New("threadpool is not running")
	}
	// For efficiency, we don't lock while writing to the queue.
	// Hence we must deal with the situation where queue is closed.
	defer func() {
		err := recover()
		if err != nil {
			log.Println("Error in BPost, recovered from ", err)
		}
	}()
	d.jobQueue <- job
	return nil
}

// Post sends a job to the threadpool only if the queue is not full.
func (d *dispatcher) Post(job Job) (err error) {
	if d == nil {
		return errors.New("Empty dispatcher")
	}
	if d.status != _running {
		return errors.New("threadpool is not running")
	}
	// For efficiency, we don't lock while writing to the queue.
	// Hence we must deal with the situation where queue is closed.
	defer func() {
		err := recover()
		if err != nil {
			log.Println("Error in Post, recovered from ", err)
		}
	}()
	select {
	case d.jobQueue <- job:
		return nil
	default:
		return errors.New("internal queue is full")
	}
}

// PendingJobs returns the num of pending jobs
func (d *dispatcher) PendingJobs() int {
	if d == nil {
		return 0
	}
	return len(d.jobQueue)
}
