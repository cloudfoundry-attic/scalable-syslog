package egress_test

import (
	"errors"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Retry Writer", func() {
	Describe("Write()", func() {
		It("calls through to a syslog writer", func() {
			writeCloser := &spyWriteCloser{
				binding: &egress.URLBinding{
					URL:     &url.URL{},
					Context: context.Background(),
				},
			}
			logClient := newSpyLogClient()
			r := buildRetryWriter(writeCloser, 1, 0, logClient)
			env := &v2.Envelope{}

			_ = r.Write(env)

			Expect(writeCloser.writeCalled).To(BeTrue())
			Expect(writeCloser.writeEnvelope).To(Equal(env))
		})

		It("retries writes if the delegation to syslog writer fails", func() {
			writeCloser := &spyWriteCloser{
				returnErrCount: 1,
				writeErr:       errors.New("write error"),
				binding: &egress.URLBinding{
					URL:     &url.URL{},
					Context: context.Background(),
				},
			}
			logClient := newSpyLogClient()
			r := buildRetryWriter(writeCloser, 3, 0, logClient)

			_ = r.Write(&v2.Envelope{})

			Eventually(writeCloser.WriteAttempts).Should(Equal(2))
		})

		It("returns an error when there are no more retries", func() {
			writeCloser := &spyWriteCloser{
				returnErrCount: 3,
				writeErr:       errors.New("write error"),
				binding: &egress.URLBinding{
					URL:     &url.URL{},
					Context: context.Background(),
				},
			}
			logClient := newSpyLogClient()
			r := buildRetryWriter(writeCloser, 2, 0, logClient)

			err := r.Write(&v2.Envelope{})

			Expect(err).To(HaveOccurred())
		})

		It("continues retrying when context is done", func() {
			ctx, cancel := context.WithCancel(context.Background())
			writeCloser := &spyWriteCloser{
				returnErrCount: 2,
				writeErr:       errors.New("write error"),
				binding: &egress.URLBinding{
					URL:     &url.URL{},
					Context: ctx,
				},
			}
			logClient := newSpyLogClient()
			r := buildRetryWriter(writeCloser, 2, 0, logClient)
			cancel()

			err := r.Write(&v2.Envelope{})

			Expect(err).To(HaveOccurred())
			Expect(writeCloser.WriteAttempts()).To(Equal(1))
		})
	})

	Describe("Close()", func() {
		It("delegates to the syslog writer", func() {
			writeCloser := &spyWriteCloser{
				binding: &egress.URLBinding{
					URL: &url.URL{},
				},
			}
			logClient := newSpyLogClient()
			r := buildRetryWriter(writeCloser, 2, 0, logClient)

			Expect(r.Close()).To(Succeed())
			Expect(writeCloser.closeCalled).To(BeTrue())
		})
	})

	Describe("ExponentialDuration", func() {
		var backoffTests = []struct {
			attempt  int
			expected time.Duration
		}{
			{0, 1000},
			{1, 1000},
			{2, 2000},
			{3, 4000},
			{4, 8000},
			{5, 16000},
			{11, 1024000},    //1.024s
			{12, 2048000},    //2.048s
			{20, 524288000},  //8m and a bit
			{21, 1048576000}, //17m28.576s
			{22, 2097152000}, //34m57.152s
		}

		It("backs off exponentially with different random seeds starting at 1ms", func() {
			rand.Seed(1)
			for _, bt := range backoffTests {
				delta := int(bt.expected / 10)

				for i := 0; i < 10; i++ {
					backoff := egress.ExponentialDuration(bt.attempt)

					Expect(bt.expected.Seconds()).To(BeNumerically("~", backoff.Seconds(), delta))
				}
			}
		})
	})
})

type spyWriteCloser struct {
	binding       *egress.URLBinding
	writeCalled   bool
	writeEnvelope *v2.Envelope
	writeAttempts int64

	returnErrCount int
	writeErr       error

	closeCalled bool
}

func (s *spyWriteCloser) Write(env *v2.Envelope) error {
	var err error
	if s.WriteAttempts() < s.returnErrCount {
		err = s.writeErr
	}
	atomic.AddInt64(&s.writeAttempts, 1)
	s.writeCalled = true
	s.writeEnvelope = env

	return err
}

func (s *spyWriteCloser) Close() error {
	s.closeCalled = true

	return nil
}

func (s *spyWriteCloser) WriteAttempts() int {
	return int(atomic.LoadInt64(&s.writeAttempts))
}

type spyLogClient struct {
	mu       sync.Mutex
	_message []string
	_appID   []string

	// We use maps to ensure that we can query the keys
	_sourceType     map[string]struct{}
	_sourceInstance map[string]struct{}
}

func newSpyLogClient() *spyLogClient {
	return &spyLogClient{
		_sourceType:     make(map[string]struct{}),
		_sourceInstance: make(map[string]struct{}),
	}
}

func (s *spyLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
	s.mu.Lock()
	defer s.mu.Unlock()

	env := &v2.Envelope{
		Tags: make(map[string]string),
	}

	for _, o := range opts {
		o(env)
	}

	s._message = append(s._message, message)
	s._appID = append(s._appID, env.SourceId)
	s._sourceType[env.GetTags()["source_type"]] = struct{}{}
	s._sourceInstance[env.GetInstanceId()] = struct{}{}
}

func (s *spyLogClient) message() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s._message
}

func (s *spyLogClient) appID() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s._appID
}

func (s *spyLogClient) sourceType() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Copy map so the orig does not escape the mutex and induce a race.
	m := make(map[string]struct{})
	for k := range s._sourceType {
		m[k] = struct{}{}
	}

	return m
}

func (s *spyLogClient) sourceInstance() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Copy map so the orig does not escape the mutex and induce a race.
	m := make(map[string]struct{})
	for k := range s._sourceInstance {
		m[k] = struct{}{}
	}

	return m
}

func buildDelay(mulitplier time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		return time.Duration(attempt) * mulitplier
	}
}

func buildRetryWriter(
	w *spyWriteCloser,
	maxRetries int,
	delayMultiplier time.Duration,
	logClient egress.LogClient,
) egress.WriteCloser {
	constructor := egress.RetryWrapper(
		func(
			binding *egress.URLBinding,
			dialTimeout time.Duration,
			ioTimeout time.Duration,
			skipCertVerify bool,
			egressMetric pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return w
		},
		egress.RetryDuration(buildDelay(delayMultiplier)),
		maxRetries,
		logClient,
	)

	return constructor(w.binding, 0, 0, false, nil)
}
