package egress_test

import (
	"errors"
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCP", func() {
	Describe("connecting to drain", func() {
		It("establishes connection to TCP drain", func() {
			mockDrain := newMockTCPDrain()
			_, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   mockDrain.Addr().String(),
			})
			Expect(err).ToNot(HaveOccurred())
			Eventually(mockDrain.ConnCount).Should(Equal(uint64(1)))
		})

		It("accepts a dialer to customize timeouts/etc", func() {
			mockDialer := newMockDialer()
			close(mockDialer.DialOutput.Conn)
			close(mockDialer.DialOutput.Err)
			_, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   "example.com:1234",
			}, egress.WithTCPDialer(mockDialer))
			Expect(err).ToNot(HaveOccurred())
			Eventually(mockDialer.DialInput.Network).Should(Receive(Equal("tcp")))
			Eventually(mockDialer.DialInput.Address).Should(Receive(Equal("example.com:1234")))
		})

		It("does not accept schemes other than syslog", func() {
			_, err := egress.NewTCP(url.URL{
				Scheme: "https",
				Host:   "example.com:1234",
			})
			Expect(err).To(HaveOccurred())
		})

		It("reconnects when initial connection fails", func() {
			mockDialer := newMockDialer()
			close(mockDialer.DialOutput.Conn)

			mockDialer.DialOutput.Err <- errors.New("i am error")
			_, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   "example.com:1234",
			}, egress.WithTCPDialer(mockDialer))
			Expect(err).ToNot(HaveOccurred())
			Eventually(mockDialer.DialCalled).Should(Receive())

			mockDialer.DialOutput.Err <- nil
			Eventually(mockDialer.DialCalled).Should(Receive())
		})
	})

	Describe("writing messages", func() {
		FIt("writes envelopes out as syslog format", func() {
			mockDrain := newMockTCPDrain()
			writer, err := egress.NewTCP(url.URL{
				Scheme: "syslog",
				Host:   mockDrain.Addr().String(),
			})
			Expect(err).ToNot(HaveOccurred())

			env := &loggregator_v2.Envelope{
				Timestamp: time.Now().UnixNano(),
				SourceId:  "source-id",
				Message: &loggregator_v2.Envelope_Log{
					Log: &loggregator_v2.Log{
						Payload: []byte("just a test"),
						Type:    loggregator_v2.Log_OUT,
					},
				},
			}
			f := func() error {
				return writer.Write(env)
			}
			Eventually(f).Should(Succeed())
			Eventually(mockDrain.RXData).Should(Equal([]byte(`\d <\d+>1 \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}([-+]\d{2}:\d{2}) org-name.space-name.app-name.1 appId \[APP/2\] - - just a test\n`)))
			//sysLogWriter.Write(standardOutPriority, []byte("just a test"), "App", "2", time.Now().UnixNano())
		})

		//It("sends messages in the proper format with source type APP/<AnyThing>", func() {
		//	sysLogWriter.Write(standardOutPriority, []byte("just a test"), "APP/PROC/BLAH", "2", time.Now().UnixNano())

		//	Eventually(syslogServerSession, 5).Should(gbytes.Say(`\d <\d+>1 \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}([-+]\d{2}:\d{2}) org-name.space-name.app-name.1 appId \[APP/PROC/BLAH/2\] - - just a test\n`))
		//}, 10)

		//It("strips null termination char from message", func() {
		//	sysLogWriter.Write(standardOutPriority, []byte(string(0)+" hi"), "appId", "", time.Now().UnixNano())

		//	Expect(syslogServerSession).ToNot(gbytes.Say("\000"))
		//})
	})
})

type mockTCPDrain struct {
	lis       net.Listener
	connCount uint64
	mu        sync.Mutex
	data      []byte
}

func newMockTCPDrain() *mockTCPDrain {
	lis, err := net.Listen("tcp", ":0")
	Expect(err).ToNot(HaveOccurred())
	m := &mockTCPDrain{
		lis: lis,
	}
	go m.accept()
	return m
}

func (m *mockTCPDrain) accept() {
	for {
		conn, err := m.lis.Accept()
		if err != nil {
			return
		}
		atomic.AddUint64(&m.connCount, 1)
		go m.handleConn(conn)
	}
}

func (m *mockTCPDrain) handleConn(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			log.Print(err)
			return
		}
		m.mu.Lock()
		m.data = append(m.data, buf[:n]...)
		m.mu.Unlock()
	}
}

func (m *mockTCPDrain) ConnCount() uint64 {
	return atomic.LoadUint64(&m.connCount)
}

func (m *mockTCPDrain) RXData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data
}

func (m *mockTCPDrain) Addr() net.Addr {
	return m.lis.Addr()
}
