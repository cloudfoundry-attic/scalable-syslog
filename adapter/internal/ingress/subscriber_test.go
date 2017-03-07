package ingress_test

import (
	"errors"
	"fmt"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/ingress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscriber", func() {
	var (
		mockClientPool *mockClientPool
		subscriber     *ingress.Subscriber
		binding        *v1.Binding
		writerBuilder  *mockWriterBuilder
	)

	BeforeEach(func() {
		mockClientPool = newMockClientPool()
		writerBuilder = newMockWriterBuilder()
		subscriber = ingress.NewSubscriber(mockClientPool, writerBuilder)
		binding = &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
	})

	It("opens a stream to an egress client", func() {
		closeWriter := newSpyCloseWriter()
		writerBuilder.BuildOutput.Cw <- closeWriter
		close(writerBuilder.BuildOutput.Err)

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
		writerBuilder.BuildOutput.Cw <- closeWriter
		writerBuilder.BuildOutput.Cw <- closeWriter
		close(writerBuilder.BuildOutput.Err)

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
		writerBuilder.BuildOutput.Cw <- closeWriter
		writerBuilder.BuildOutput.Cw <- closeWriter
		close(writerBuilder.BuildOutput.Err)

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
		writerBuilder.BuildOutput.Cw <- closeWriter
		writerBuilder.BuildOutput.Cw <- closeWriter
		close(writerBuilder.BuildOutput.Err)

		client := newMockEgressClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)

		unsubscribeFn := subscriber.Start(binding)

		Eventually(mockClientPool.NextCalled).Should(Receive())

		close(receiverClient.RecvOutput.Ret1)
		go func() {
			for {
				receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
			}
		}()

		unsubscribeFn()

		Eventually(receiverClient.CloseSendCalled).Should(Receive())
		Eventually(closeWriter.closes).Should(Receive())

		Consistently(mockClientPool.NextCalled).ShouldNot(Receive())
	})

	It("ignores non log messages", func() {
		closeWriter := newSpyCloseWriter()
		writerBuilder.BuildOutput.Cw <- closeWriter
		writerBuilder.BuildOutput.Cw <- closeWriter
		close(writerBuilder.BuildOutput.Err)

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
