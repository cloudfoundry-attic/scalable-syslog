package ingress_test

import (
	"errors"
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client Manager", func() {
	Context("After a period time", func() {
		It("rolls the connections", func() {
			mockConnector := NewMockConnector()
			ingress.NewClientManager(
				mockConnector,
				5,
				10*time.Millisecond,
				1*time.Millisecond,
				ingress.WithRetryWait(10*time.Millisecond),
			)

			Eventually(mockConnector.GetSuccessfulConnections).Should(Equal(5))
			Eventually(mockConnector.GetCloseCalled).Should(BeNumerically(">", 5))
			Eventually(mockConnector.GetSuccessfulConnections).Should(Equal(5))
		})
	})

	Describe("Next()", func() {
		It("returns a client", func() {
			mockConnector := NewMockConnector()
			cm := ingress.NewClientManager(
				mockConnector,
				5,
				10*time.Millisecond,
				1*time.Millisecond,
				ingress.WithRetryWait(10*time.Millisecond),
			)

			Eventually(mockConnector.GetSuccessfulConnections).Should(Equal(5))

			r1 := cm.Next()
			r2 := cm.Next()

			Expect(r1).ToNot(BeIdenticalTo(r2))
		})

		Context("when connector fails", func() {
			It("does not return a nil client", func() {
				mockConnector := NewMockConnector()

				for i := 0; i < 15; i++ {
					mockConnector.connectErrors <- errors.New("an-error")
				}
				cm := ingress.NewClientManager(
					mockConnector,
					5,
					10*time.Millisecond,
					1*time.Millisecond,
					ingress.WithRetryWait(10*time.Millisecond),
				)

				r1 := cm.Next()
				Expect(r1).ToNot(BeNil())
			})

			It("it attempts to reconnect", func() {
				mockConnector := NewMockConnector()

				for i := 0; i < 15; i++ {
					mockConnector.connectErrors <- errors.New("an-error")
				}
				ingress.NewClientManager(
					mockConnector,
					5,
					time.Hour,
					1*time.Millisecond,
					ingress.WithRetryWait(10*time.Millisecond),
				)

				Eventually(mockConnector.GetConnectCalled).Should(BeNumerically(">", 5))
			})
		})
	})
})

type MockConnector struct {
	connectCalled         int
	closeCalled           int
	successfulConnections int
	mu                    sync.Mutex
	connectErrors         chan error
}

func NewMockConnector() *MockConnector {
	return &MockConnector{
		connectErrors: make(chan error, 100),
	}
}

func (m *MockConnector) Connect() (io.Closer, ingress.LogsProviderClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectCalled++
	m.successfulConnections++

	var err error
	if len(m.connectErrors) > 0 {
		err = <-m.connectErrors
	}

	return m, &MockReceiver{}, err
}

func (m *MockConnector) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closeCalled++
	m.successfulConnections--

	return nil
}

func (m *MockConnector) GetSuccessfulConnections() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.successfulConnections
}

func (m *MockConnector) GetCloseCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.closeCalled
}

func (m *MockConnector) GetConnectCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.connectCalled
}

type MockReceiver struct {
	n int
}

func (m *MockReceiver) Receiver(ctx context.Context, in *v2.EgressRequest) (v2.Egress_ReceiverClient, error) {
	return nil, nil
}
