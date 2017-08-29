package ingress_test

import (
	"errors"
	"fmt"
	"time"

	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscriber", func() {
	var (
		mockClientPool  *mockClientPool
		spyEmitter      *testhelper.SpyMetricClient
		subscriber      *ingress.Subscriber
		binding         *v1.Binding
		syslogConnector *mockSyslogConnector
	)

	BeforeEach(func() {
		mockClientPool = newMockClientPool()
		spyEmitter = testhelper.NewMetricClient()
		syslogConnector = newMockSyslogConnector()
		subscriber = ingress.NewSubscriber(
			context.TODO(),
			mockClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)
		binding = &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
	})

	It("opens a stream to a batching egress client", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client

		batchedReceiverClient := newMockBatchedReceiverClient()
		client.BatchedReceiverOutput.Ret0 <- batchedReceiverClient
		close(client.BatchedReceiverOutput.Ret1)

		subscriber.Start(binding)

		var request *v2.EgressBatchRequest
		Eventually(client.BatchedReceiverInput.In).Should(Receive(&request))
		Expect(request.Filter.GetSourceId()).To(Equal(binding.AppId))
		Expect(request.Filter.GetLog()).ToNot(BeNil())
		Expect(request.ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))
		Expect(request.UsePreferredTags).To(BeTrue())

		Eventually(batchedReceiverClient.RecvCalled).Should(Receive())
		close(batchedReceiverClient.RecvOutput.Ret1)
		batchedReceiverClient.RecvOutput.Ret0 <- buildBatchedLogs(3)

		Eventually(closeWriter.writes).Should(HaveLen(3))
	})

	It("opens a stream to an egress client when batching is unavailable", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(client.BatchedReceiverOutput.Ret0)
		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.BatchedReceiverOutput.Ret1 <- unimplemented

		subscriber.Start(binding)

		var request *v2.EgressRequest
		Eventually(client.ReceiverInput.In).Should(Receive(&request))
		Expect(request.Filter.GetSourceId()).To(Equal(binding.AppId))
		Expect(request.Filter.GetLog()).ToNot(BeNil())
		Expect(request.ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))
		Expect(request.UsePreferredTags).To(BeTrue())

		Eventually(receiverClient.RecvCalled).Should(Receive())
		close(receiverClient.RecvOutput.Ret1)
		for i := 0; i < 3; i++ {
			receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
		}

		Eventually(closeWriter.writes).Should(HaveLen(3))
	})

	It("uses another egress client if it fails to open a stream", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.W <- closeWriter
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		client.BatchedReceiverOutput.Ret1 <- errors.New("no batched receiver")
		close(client.BatchedReceiverOutput.Ret0)
		close(client.ReceiverOutput.Ret0)
		client.ReceiverOutput.Ret1 <- errors.New("some-error")

		subscriber.Start(binding)
		Eventually(mockClientPool.NextCalled).Should(HaveLen(2))
	})

	It("gets a new client and receiver if Recv() fails", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.W <- closeWriter
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)
		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.BatchedReceiverOutput.Ret1 <- unimplemented
		client.BatchedReceiverOutput.Ret1 <- unimplemented
		close(client.BatchedReceiverOutput.Ret0)

		subscriber.Start(binding)

		By("successfully receiving")
		receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()
		receiverClient.RecvOutput.Ret1 <- nil

		By("failing to receive")
		receiverClient.RecvOutput.Ret0 <- nil
		receiverClient.RecvOutput.Ret1 <- errors.New("an-error")

		Eventually(receiverClient.CloseSendCalled).Should(Receive())

		var ctx context.Context
		Eventually(syslogConnector.ConnectInput.Ctx).Should(Receive(&ctx))
		Eventually(ctx.Done).Should(BeClosed())

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
		syslogConnector.ConnectOutput.W <- closeWriter
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)
		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.BatchedReceiverOutput.Ret1 <- unimplemented
		close(client.BatchedReceiverOutput.Ret0)

		unsubscribe := subscriber.Start(binding)

		Eventually(mockClientPool.NextCalled).Should(Receive())

		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case receiverClient.RecvOutput.Ret0 <- buildLogEnvelope():
					// Do nothing
				case <-done:
					return
				}
			}
		}()

		unsubscribe()
		receiverClient.RecvOutput.Ret1 <- errors.New("some-error")

		Eventually(receiverClient.CloseSendCalled).Should(Receive())
		Consistently(mockClientPool.NextCalled).ShouldNot(Receive())

		var ctx context.Context
		Eventually(syslogConnector.ConnectInput.Ctx).Should(Receive(&ctx))
		Eventually(ctx.Done).Should(BeClosed())

		Eventually(client.ReceiverInput.Ctx).Should(Receive(&ctx))
		Eventually(ctx.Done).Should(BeClosed())
	})

	It("ignores non log messages", func() {
		closeWriter := newSpyCloseWriter()
		syslogConnector.ConnectOutput.W <- closeWriter
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)
		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.BatchedReceiverOutput.Ret1 <- unimplemented
		close(client.BatchedReceiverOutput.Ret0)

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
		syslogConnector.ConnectOutput.W <- closeWriter
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client

		receiverClient := newMockReceiverClient()
		client.ReceiverOutput.Ret0 <- receiverClient
		close(client.ReceiverOutput.Ret1)
		close(receiverClient.CloseSendOutput.Ret0)
		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.BatchedReceiverOutput.Ret1 <- unimplemented
		close(client.BatchedReceiverOutput.Ret0)

		subscriber.Start(binding)

		By("receiving a log message")
		close(receiverClient.RecvOutput.Ret1)
		receiverClient.RecvOutput.Ret0 <- buildLogEnvelope()

		Eventually(func() uint64 {
			return spyEmitter.GetDelta("ingress")
		}).Should(Equal(uint64(1)))
	})

	It("times out opening a stream", func() {
		syslogConnector.ConnectOutput.W <- newSpyCloseWriter()
		syslogConnector.ConnectOutput.W <- newSpyCloseWriter()
		close(syslogConnector.ConnectOutput.Err)

		client := newMockLogsProviderClient()
		mockClientPool.NextOutput.Client <- client

		subscriber.Start(binding)
		Eventually(mockClientPool.NextCalled).Should(Receive())

		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.BatchedReceiverOutput.Ret1 <- unimplemented
		close(client.BatchedReceiverOutput.Ret0)
		close(client.ReceiverOutput.Ret0)
		ctx := <-client.ReceiverInput.Ctx
		Eventually(ctx.Done).Should(BeClosed())

		client.ReceiverOutput.Ret1 <- errors.New("Stream Open Failed")
		Eventually(mockClientPool.NextCalled).Should(Receive())
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

type spyBatchedReceiverClient struct {
}

func newSpyBatchedReceiverClient() *spyBatchedReceiverClient {
	return &spyBatchedReceiverClient{}
}
func buildLogEnvelope() *v2.Envelope {
	return &v2.Envelope{
		Tags: map[string]string{
			"source_type":     "APP",
			"source_instance": "2",
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
func buildBatchedLogs(size int) *v2.EnvelopeBatch {
	batch := &v2.EnvelopeBatch{
		Batch: make([]*v2.Envelope, 0),
	}

	for i := 0; i < size; i++ {
		env := buildLogEnvelope()
		batch.Batch = append(batch.Batch, env)
	}

	return batch
}

// func buildBatchedCounter(size int) *v2.EnvelopeBatch {
// 	batch := &v2.EnvelopeBatch{
// 		Batch: make([]*v2.Envelope, 0),
// 	}

// 	for i := 0; i < size; i++ {
// 		env := buildCounterEnvelope()
// 		batch.Batch = append(batch.Batch, env)
// 	}
// 	fmt.Printf("your batched counter: %#v", batch)
// 	return batch
// }
