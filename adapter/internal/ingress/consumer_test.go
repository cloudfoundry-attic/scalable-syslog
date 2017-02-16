package ingress_test

import (
	"io"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

//TODO: test TLS config from app_test.go

var _ = Describe("Consumer", func() {
	Context("After a period time", func() {
		It("rolls the connections", func() {
			mockConnector := NewMockConnector()
			ingress.NewConsumer(mockConnector, 5, 10*time.Millisecond)

			Eventually(func() int {
				return mockConnector.connectCalled
			}).Should(Equal(5))

			Eventually(func() int {
				return mockConnector.connectCalled
			}, time.Second, time.Millisecond).Should(BeNumerically("<", 5))

			Eventually(func() int {
				return mockConnector.connectCalled
			}).Should(Equal(5))
		})
	})
})

type MockConnector struct {
	connectCalled int
}

func NewMockConnector() *MockConnector {
	return new(MockConnector)
}

// type Egress_ReceiverClient interface {
// 	Recv() (*Envelope, error)
// 	grpc.ClientStream
// }

func (m *MockConnector) Connect() (io.Closer, v2.Egress_ReceiverClient) {
	m.connectCalled++

	return m, nil
}

func (m *MockConnector) Close() error {
	m.connectCalled--
	time.Sleep(1 * time.Millisecond)

	return nil
}
