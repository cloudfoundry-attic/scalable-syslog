package egress_test

import (
	"errors"
	"math/rand"
	"net/url"
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
			logClient := &spyLogClient{}
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
			logClient := &spyLogClient{}
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
			logClient := &spyLogClient{}
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
			logClient := &spyLogClient{}
			r := buildRetryWriter(writeCloser, 2, 0, logClient)
			cancel()

			err := r.Write(&v2.Envelope{})

			Expect(err).To(HaveOccurred())
			Expect(writeCloser.WriteAttempts()).To(Equal(1))
		})

		It("returns an error if the context is canceled", func(done Done) {
			ctx, cancel := context.WithCancel(context.Background())
			writeCloser := &spyWriteCloser{
				returnErrCount: 2,
				writeErr:       errors.New("write error"),
				binding: &egress.URLBinding{
					URL:     &url.URL{},
					Context: ctx,
				},
			}
			logClient := &spyLogClient{}
			r := buildRetryWriter(writeCloser, 5, time.Millisecond, logClient)

			go func() {
				Eventually(writeCloser.WriteAttempts).Should(Equal(1))
				cancel()
				done <- struct{}{}
			}()

			err := r.Write(&v2.Envelope{})
			Expect(err).To(HaveOccurred())
			Expect(writeCloser.WriteAttempts()).To(Equal(2))
			Eventually(done).Should(Receive())
		})

		It("writes out the LGR message", func() {
			writeCloser := &spyWriteCloser{
				returnErrCount: 1,
				writeErr:       errors.New("write error"),
				binding: &egress.URLBinding{
					URL:     &url.URL{},
					AppID:   "some-app-id",
					Context: context.Background(),
				},
			}
			logClient := &spyLogClient{}
			r := buildRetryWriter(writeCloser, 2, 0, logClient)

			_ = r.Write(&v2.Envelope{Tags: map[string]*v2.Value{
				"source_instance": &v2.Value{
					Data: &v2.Value_Text{
						Text: "a source instance",
					},
				},
			}})

			Expect(logClient.calledWith).To(Equal("Syslog Drain: Error when writing. Backing off for 0s."))
			Expect(logClient.appID).To(Equal("some-app-id"))
			Expect(logClient.sourceType).To(Equal("LGR"))
			Expect(logClient.sourceInstance).To(Equal("a source instance"))
		})
	})

	Describe("Close()", func() {
		It("delegates to the syslog writer", func() {
			writeCloser := &spyWriteCloser{
				binding: &egress.URLBinding{
					URL: &url.URL{},
				},
			}
			logClient := &spyLogClient{}
			r := buildRetryWriter(writeCloser, 2, 0, logClient)

			Expect(r.Close()).To(Succeed())
			Expect(writeCloser.closeCalled).To(BeTrue())
		})
	})

	Describe("ExponentialDuration", func() {
		var backoffTests = []struct {
			attempt  uint
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
	calledWith     string
	appID          string
	sourceType     string
	sourceInstance string
}

func (s *spyLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
	s.calledWith = message
	env := &v2.Envelope{
		Tags: make(map[string]*v2.Value),
	}
	for _, o := range opts {
		o(env)
	}
	s.appID = env.SourceId
	s.sourceType = env.GetTags()["source_type"].GetText()
	s.sourceInstance = env.GetTags()["source_instance"].GetText()
}

func buildDelay(mulitplier time.Duration) func(uint) time.Duration {
	return func(attempt uint) time.Duration {
		return time.Duration(attempt) * mulitplier
	}
}

func buildRetryWriter(
	w *spyWriteCloser,
	maxRetries uint,
	delayMultiplier time.Duration,
	logClient egress.LogClient,
) egress.WriteCloser {
	constructor := egress.RetryWrapper(
		func(
			binding *egress.URLBinding,
			dialTimeout time.Duration,
			ioTimeout time.Duration,
			skipCertVerify bool,
			egressMetric *pulseemitter.CounterMetric,
		) egress.WriteCloser {
			return w
		},
		egress.RetryDuration(buildDelay(delayMultiplier)),
		maxRetries,
		logClient,
	)

	return constructor(w.binding, 0, 0, false, nil)
}
