package ingress_test

import (
	"errors"
	"io"
	"sync"
	"time"

	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client Manager", func() {
	It("rolls the connections after a TTL", func() {
		connector := newSpyConnector()
		ingress.NewClientManager(
			connector,
			5,
			10*time.Millisecond,
			1*time.Millisecond,
			ingress.WithRetryWait(10*time.Millisecond),
		)

		Eventually(connector.connectionCount).Should(Equal(5))
		Eventually(connector.closeCalled).Should(BeNumerically(">", 5))
		Eventually(connector.connectionCount).Should(Equal(5))
	})

	It("rolls the connections when invalid", func() {
		connector := newSpyConnector()
		connector.receiver.Invalidate()

		ingress.NewClientManager(
			connector,
			5,
			time.Hour,
			time.Millisecond,
			ingress.WithRetryWait(10*time.Millisecond),
		)

		Eventually(connector.connectionCount).Should(Equal(5))
		Eventually(connector.closeCalled).Should(BeNumerically(">", 5))
		Eventually(connector.connectionCount).Should(Equal(5))
	})

	It("returns a client", func() {
		connector := newSpyConnector()

		// This forces the connector to create a new receiver each
		// time.
		connector.receiver = nil

		cm := ingress.NewClientManager(
			connector,
			5,
			10*time.Millisecond,
			1*time.Millisecond,
			ingress.WithRetryWait(10*time.Millisecond),
		)

		Eventually(connector.connectionCount).Should(Equal(5))

		r1 := cm.Next()
		r2 := cm.Next()

		Expect(r1).ToNot(BeIdenticalTo(r2))
	})

	It("does not return a nil client when connector fails", func() {
		connector := newSpyConnector()

		for i := 0; i < 15; i++ {
			connector.connectErrors <- errors.New("an-error")
		}
		cm := ingress.NewClientManager(
			connector,
			5,
			10*time.Millisecond,
			1*time.Millisecond,
			ingress.WithRetryWait(10*time.Millisecond),
		)

		r1 := cm.Next()
		Expect(r1).ToNot(BeNil())
	})

	It("it attempts to reconnect when connector fails", func() {
		connector := newSpyConnector()

		for i := 0; i < 15; i++ {
			connector.connectErrors <- errors.New("an-error")
		}
		ingress.NewClientManager(
			connector,
			5,
			time.Hour,
			1*time.Millisecond,
			ingress.WithRetryWait(10*time.Millisecond),
		)

		Eventually(connector.connectCalled).Should(BeNumerically(">", 5))
	})
})

type spyConnector struct {
	connectCalled_        int
	closeCalled_          int
	successfulConnections int
	mu                    sync.Mutex
	connectErrors         chan error
	receiver              *spyReceiver
}

func newSpyConnector() *spyConnector {
	return &spyConnector{
		connectErrors: make(chan error, 100),
		receiver:      &spyReceiver{},
	}
}

func (s *spyConnector) Connect() (io.Closer, ingress.LogsProviderClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connectCalled_++
	s.successfulConnections++

	var err error
	if len(s.connectErrors) > 0 {
		err = <-s.connectErrors
	}

	if s.receiver == nil {
		return s, &spyReceiver{}, err
	}

	return s, s.receiver, err
}

func (s *spyConnector) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeCalled_++
	s.successfulConnections--

	return nil
}

func (s *spyConnector) connectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.successfulConnections
}

func (s *spyConnector) closeCalled() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeCalled_
}

func (s *spyConnector) connectCalled() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.connectCalled_
}

type spyReceiver struct {
	n       int
	invalid bool
}

func (s *spyReceiver) Valid() bool {
	return !s.invalid
}

func (s *spyReceiver) Invalidate() {
	s.invalid = true
}

func (s *spyReceiver) BatchedReceiver(
	context.Context,
	*v2.EgressBatchRequest,
	...grpc.CallOption,
) (v2.Egress_BatchedReceiverClient, error) {
	return nil, nil
}
