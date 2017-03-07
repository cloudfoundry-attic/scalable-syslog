package ingress_test

import (
	"errors"
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client Manager", func() {
	Context("After a period time", func() {
		It("rolls the connections", func() {
			mockConnector := NewMockConnector()
			ingress.NewClientManager(mockConnector, 5, 10*time.Millisecond)

			Eventually(func() int {
				return mockConnector.GetSuccessfulConnections()
			}).Should(Equal(5))

			Eventually(func() int {
				return mockConnector.GetCloseCalled()
			}).Should(BeNumerically(">", 5))

			Eventually(func() int {
				return mockConnector.GetSuccessfulConnections()
			}).Should(Equal(5))
		})
	})

	Describe("Next()", func() {
		It("returns a client", func() {
			mockConnector := NewMockConnector()
			cm := ingress.NewClientManager(mockConnector, 5, 10*time.Millisecond)

			Eventually(func() int {
				return mockConnector.GetSuccessfulConnections()
			}).Should(Equal(5))

			r1 := cm.Next()
			r2 := cm.Next()

			Expect(r1).ToNot(BeIdenticalTo(r2))
		})
	})
})

type MockConnector struct {
	connectCalled         int
	closeCalled           int
	successfulConnections int
	mu                    sync.Mutex
}

func NewMockConnector() *MockConnector {
	return new(MockConnector)
}

func (m *MockConnector) Connect() (io.Closer, v2.EgressClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectCalled++

	if m.connectCalled < 10 {
		return nil, nil, errors.New("Getting client failed")
	}

	m.successfulConnections++

	return m, new(MockReceiver), nil
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

type MockReceiver struct {
	n int
}

func (m *MockReceiver) Receiver(ctx context.Context, in *v2.EgressRequest, opts ...grpc.CallOption) (v2.Egress_ReceiverClient, error) {
	return nil, nil
}
