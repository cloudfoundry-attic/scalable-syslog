package egress_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
)

var _ = Describe("DiodeWriter", func() {
	It("dispatches calls to write to the underlying writer", func() {
		expectedEnv := &loggregator_v2.Envelope{
			SourceId: "test-source-id",
		}
		spyWriter := &SpyWriter{}
		spyAlerter := &SpyAlerter{}
		dw := egress.NewDiodeWriter(spyWriter, spyAlerter)

		dw.Write(expectedEnv)

		Eventually(spyWriter.calledWith).Should(Equal([]*loggregator_v2.Envelope{
			expectedEnv,
		}))
	})

	It("dispatches calls to close to the underlying writer", func() {
		spyWriter := &SpyWriter{}
		spyAlerter := &SpyAlerter{}
		dw := egress.NewDiodeWriter(spyWriter, spyAlerter)

		dw.Close()

		Expect(spyWriter.closeCalled).To(Equal(1))
	})

	It("returns the underlying writer's close error", func() {
		expectedErr := errors.New("test-close-error")
		spyWriter := &SpyWriter{
			closeRet: expectedErr,
		}
		spyAlerter := &SpyAlerter{}
		dw := egress.NewDiodeWriter(spyWriter, spyAlerter)

		err := dw.Close()

		Expect(err).To(Equal(expectedErr))
	})

	It("is not blocked when underlying writer is blocked", func(done Done) {
		defer close(done)
		spyWriter := &SpyWriter{
			blockWrites: true,
		}
		spyAlerter := &SpyAlerter{}
		dw := egress.NewDiodeWriter(spyWriter, spyAlerter)
		dw.Write(nil)
	})
})

type SpyWriter struct {
	mu          sync.Mutex
	calledWith_ []*loggregator_v2.Envelope
	closeCalled int
	closeRet    error
	blockWrites bool
}

func (s *SpyWriter) Write(env *loggregator_v2.Envelope) error {
	if s.blockWrites {
		for {
			time.Sleep(100 * time.Millisecond)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calledWith_ = append(s.calledWith_, env)
	return nil
}

func (s *SpyWriter) Close() error {
	s.closeCalled++
	return s.closeRet
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
