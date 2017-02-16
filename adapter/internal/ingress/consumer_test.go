package ingress_test

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	Context("After a period time", func() {
		It("rolls the connections", func() {
			mockConnector := NewMockConnector()
			ingress.NewConsumer(mockConnector, 5, 10*time.Millisecond)

			Eventually(func() int {
				return mockConnector.GetSuccessfulConnections()
			}).Should(Equal(5))

			Eventually(func() int {
				return mockConnector.GetSuccessfulConnections()
			}, time.Second, time.Millisecond).Should(BeNumerically("<", 5))

			Eventually(func() int {
				return mockConnector.GetSuccessfulConnections()
			}).Should(Equal(5))
		})
	})
})

type MockConnector struct {
	connectCalled         int
	successfulConnections int
	mu                    sync.Mutex
}

func NewMockConnector() *MockConnector {
	return new(MockConnector)
}

func (m *MockConnector) Connect() (io.Closer, v2.Egress_ReceiverClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectCalled++

	if m.connectCalled < 10 {
		return nil, nil, errors.New("Getting client failed")
	}

	m.successfulConnections++

	return m, nil, nil
}

func (m *MockConnector) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.successfulConnections--
	time.Sleep(1 * time.Millisecond)

	return nil
}

func (m *MockConnector) GetSuccessfulConnections() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.successfulConnections
}
