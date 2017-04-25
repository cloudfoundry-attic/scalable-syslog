package ingress_test

import (
	"errors"
	"fmt"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/metric"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscriber", func() {
	var (
		mockClientPool  *mockClientPool
		spyEmitter      *spyMetricEmitter
		subscriber      *ingress.Subscriber
		binding         *v1.Binding
		syslogConnector *mockSyslogConnector
	)

	BeforeEach(func() {
		mockClientPool = newMockClientPool()
		spyEmitter = newSpyMetricEmitter()
		syslogConnector = newMockSyslogConnector()
		subscriber = ingress.NewSubscriber(mockClientPool, syslogConnector, spyEmitter)
		binding = &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
	})

	It("opens a stream to an egress client", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.Cw <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)

		subscriber.Start(binding)

		var request *v2.EgressRequest
		Eventually(client.ReceiverInput.In).Should(Receive(&request))
		Expect(request.Filter).To(Equal(&v2.Filter{binding.AppId}))
		Expect(request.ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))

		Eventually(receiverClient.RecvCalled).Should(Receive())
		close(receiverClient.RecvOutput.Ret1)
		for i := 0; i < 3; i++ {
			receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
		}

		Eventually(closeWriter.writes).Should(HaveLen(3))
	})

	It("uses another egress client if it fails to open a stream", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.Cw <- closeWriter
		syslogConnector.ConnectOutput.Cw <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		close(client.ReceiverOutput.Ret0)
		client.ReceiverOutput.Ret1 <- errors.New("some-error")

		subscriber.Start(binding)
		Eventually(mockClientPool.NextCalled).Should(HaveLen(2))
	})

	It("gets a new client and reciever if Recv() fails", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.Cw <- closeWriter
		syslogConnector.ConnectOutput.Cw <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)

		subscriber.Start(binding)

		By("successfully receiving")
		receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
		receiverClient.RecvOutput.Ret1 <- nil

		By("failing to receive")
		receiverClient.RecvOutput.Ret0 <- nil
		receiverClient.RecvOutput.Ret1 <- errors.New("an-error")

		Eventually(receiverClient.CloseSendCalled).Should(Receive())
		Eventually(closeWriter.closes).Should(Receive())

		By("get a new client and receiver")
		Eventually(mockClientPool.NextCalled).Should(HaveLen(2))
		Eventually(client.ReceiverCalled).Should(HaveLen(2))

		By("receiving more logs")
		Eventually(receiverClient.RecvCalled).Should(Receive())
		close(receiverClient.RecvOutput.Ret1)
		for i := 0; i < 3; i++ {
			receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
		}

		Eventually(closeWriter.writes).Should(HaveLen(4))
	})

	It("closes all connections when the unsubscribe func is called", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.Cw <- closeWriter
		syslogConnector.ConnectOutput.Cw <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)
		close(receiverClient.RecvOutput.Ret1)

		unsubscribe := subscriber.Start(binding)

		Eventually(mockClientPool.NextCalled).Should(Receive())

		go func() {
			for {
				select {
				case receiverClient.RecvOutput.Ret0 <- buildLogEnvelope():
					// Do nothing
				default:
					return
				}
			}
		}()

		unsubscribe()

		Eventually(receiverClient.CloseSendCalled).Should(Receive())
		Eventually(closeWriter.closes).Should(Receive())
		Consistently(mockClientPool.NextCalled).ShouldNot(Receive())
	})

	It("cancels the connection context if unsubscribe func is called", func() {
		syslogConnector.ConnectOutput.Cw <- newSpyCloseWriter()
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.RecvOutput.Ret0)
		close(receiverClient.RecvOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)

		unsubscribe := subscriber.Start(binding)
		unsubscribe()

		ctx := <-client.ReceiverInput.Ctx
		done := ctx.Done()
		Eventually(done, 2).Should(BeClosed())
	})

	It("ignores non log messages", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.Cw <- closeWriter
		syslogConnector.ConnectOutput.Cw <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)

		subscriber.Start(binding)

		By("receiving a log message")
		close(receiverClient.RecvOutput.Ret1)
		receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()

		By("receiving non log messages")
		receiverClient.RecvOutput.Ret0 <- buildCounterEnvelope()
		receiverClient.RecvOutput.Ret0 <- buildCounterEnvelope()

		Eventually(closeWriter.writes).Should(HaveLen(1))
		Consistently(closeWriter.writes).Should(HaveLen(1))
	})

	It("emits ingress metrics", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.Cw <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)

		subscriber.Start(binding)

		By("receiving a log message")
		close(receiverClient.RecvOutput.Ret1)
		go func() {
			for {
				receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
			}
		}()

		go func() {
			// drain the relevant chans
			for {
				select {
				case <-closeWriter.writes:
				case <-receiverClient.RecvCalled:
				}
			}
		}()

		Eventually(spyEmitter.names).Should(Receive(Equal("ingress")))
		Expect(spyEmitter.opts).To(Receive(HaveLen(2)))
	})
})

type spyCloseWriter struct {
	writes chan *v2.Envelope
	closes chan bool
}

func newSpyCloseWriter() *spyCloseWriter {
	return &spyCloseWriter{
		writes: make(chan *v2.Envelope, 100),
		closes: make(chan bool, 100),
	}
}

func (s *spyCloseWriter) Write(env *v2.Envelope) error {
	s.writes <- env
	return nil
}

func (s *spyCloseWriter) Close() error {
	s.closes <- true
	return nil
}

type spyMetricEmitter struct {
	names chan string
	opts  chan []metric.IncrementOpt
}

func newSpyMetricEmitter() *spyMetricEmitter {
	return &spyMetricEmitter{
		names: make(chan string, 100),
		opts:  make(chan []metric.IncrementOpt, 100),
	}
}

func (e *spyMetricEmitter) IncCounter(name string, options ...metric.IncrementOpt) {
	e.names <- name
	e.opts <- options
}

func buildLogEnvelope() *v2.Envelope {
	return &v2.Envelope{
		Tags: map[string]*v2.Value{
			"source_type":     {&v2.Value_Text{"APP"}},
			"source_instance": {&v2.Value_Text{"2"}},
		},
		Timestamp: 12345678,
		SourceId:  "source-id",
		Message: &v2.Envelope_Log{
			Log: &v2.Log{
				Payload: []byte("log"),
				Type:    v2.Log_OUT,
			},
		},
	}
}

func buildCounterEnvelope() *v2.Envelope {
	return &v2.Envelope{
		Timestamp: 12345678,
		SourceId:  "source-id",
		Message: &v2.Envelope_Counter{
			Counter: &v2.Counter{},
		},
	}
}
