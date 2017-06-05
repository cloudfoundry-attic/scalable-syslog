package timeoutwaitgroup

import (
	"sync"
	"time"
)

type TimeoutWaitGroup struct {
	wg      sync.WaitGroup
	timeout time.Duration
}

func New(timeout time.Duration) *TimeoutWaitGroup {
	return &TimeoutWaitGroup{
		timeout: timeout,
	}
}

func (s *TimeoutWaitGroup) Wait() {
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(s.timeout):
	}
}

func (s *TimeoutWaitGroup) Add(delta int) {
	s.wg.Add(delta)
}

func (s *TimeoutWaitGroup) Done() {
	s.wg.Done()
}
