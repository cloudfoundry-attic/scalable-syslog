package egress_test

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
)

var _ = Describe("DiodeWriter", func() {
	It("dispatches calls to write to the underlying writer", func() {
		expectedEnv := &loggregator_v2.Envelope{
			SourceId: "test-source-id",
		}
		spyWriter := &SpyWriter{}
		spyAlerter := &SpyAlerter{}
		dw := egress.NewDiodeWriter(context.TODO(), spyWriter, spyAlerter)

		dw.Write(expectedEnv)

		Eventually(spyWriter.calledWith).Should(Equal([]*loggregator_v2.Envelope{
			expectedEnv,
		}))
	})

	It("dispatches calls to close to the underlying writer", func() {
		spyWriter := &SpyWriter{}
		spyAlerter := &SpyAlerter{}
		ctx, cancel := context.WithCancel(context.TODO())

		egress.NewDiodeWriter(ctx, spyWriter, spyAlerter)

		cancel()

		Eventually(spyWriter.CloseCalled).Should(Equal(int64(1)))
	})

	It("is not blocked when underlying writer is blocked", func(done Done) {
		defer close(done)
		spyWriter := &SpyWriter{
			blockWrites: true,
		}
		spyAlerter := &SpyAlerter{}
		dw := egress.NewDiodeWriter(context.TODO(), spyWriter, spyAlerter)
		dw.Write(nil)
	})

	It("flushes existing messages after close", func() {
		spyWriter := &SpyWriter{
			blockWrites: true,
		}
		spyAlerter := &SpyAlerter{}
		ctx, cancel := context.WithCancel(context.TODO())

		dw := egress.NewDiodeWriter(ctx, spyWriter, spyAlerter)

		e := &loggregator_v2.Envelope{}
		for i := 0; i < 100; i++ {
			dw.Write(e)
		}
		cancel()
		spyWriter.WriteBlocked(false)

		Eventually(spyWriter.calledWith).Should(HaveLen(100))
	})
})

type SpyWriter struct {
	mu          sync.Mutex
	calledWith_ []*loggregator_v2.Envelope
	closeCalled int64
	closeRet    error
	blockWrites bool
}

func (s *SpyWriter) Write(env *loggregator_v2.Envelope) error {
	for {
		s.mu.Lock()
		block := s.blockWrites
		s.mu.Unlock()

		if block {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		break
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calledWith_ = append(s.calledWith_, env)
	return nil
}

func (s *SpyWriter) WriteBlocked(blocked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.blockWrites = blocked
}

func (s *SpyWriter) Close() error {
	atomic.AddInt64(&s.closeCalled, 1)

	return s.closeRet
}

func (s *SpyWriter) CloseCalled() int64 {
	return atomic.LoadInt64(&s.closeCalled)
}

func (s *SpyWriter) calledWith() []*loggregator_v2.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calledWith_
}

type SpyAlerter struct {
	missed_ int64
}

func (s *SpyAlerter) Alert(missed int) {
	atomic.AddInt64(&s.missed_, int64(missed))
}

func (s *SpyAlerter) missed() int64 {
	return atomic.LoadInt64(&s.missed_)
}
