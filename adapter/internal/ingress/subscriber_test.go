package ingress_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscriber", func() {
	It("opens a stream with a batching egress client", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		spyClientPool.next = client
		batchedReceiverClient := newSpyBatchedReceiverClient()
		batchedReceiverClient.recv = buildBatchedLogs(3)
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		subscriber.Start(binding)

		Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())
		Expect(client.batchedReceiverRequest().Filter.GetSourceId()).To(Equal(binding.AppId))
		Expect(client.batchedReceiverRequest().Filter.GetLog()).ToNot(BeNil())
		Expect(client.batchedReceiverRequest().ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))
		Expect(client.batchedReceiverRequest().UsePreferredTags).To(BeTrue())
		Eventually(writer.writes).Should(Equal(3))
	})

	It("opens a stream with an egress client when batching is unavailable", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		spyClientPool.next = client
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		client.batchedReceiverError = status.Error(codes.Unimplemented, "unimplemented")
		receiver := newSpyReceiverClient()
		receiver.recv = buildLogEnvelope()
		client.receiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(client.receiverRequest).ShouldNot(BeNil())
		Expect(client.receiverRequest().Filter.GetSourceId()).To(Equal(binding.AppId))
		Expect(client.receiverRequest().Filter.GetLog()).ToNot(BeNil())
		Expect(client.receiverRequest().ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))
		Expect(client.receiverRequest().UsePreferredTags).To(BeTrue())
		Eventually(writer.writes).Should(Equal(1))
	})

	It("acquires another client when one client fails", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		spyClientPool.next = client
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		client.batchedReceiverError = errors.New("cannot get batcher")
		receiver := newSpyReceiverClient()
		receiver.recv = buildLogEnvelope()
		client.receiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(spyClientPool.nextCalls).Should(BeNumerically(">", 1))
	})

	It("acquires another receiver when Recv() fails", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		spyClientPool.next = client
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		unimplemented := status.Error(codes.Unimplemented, "unimplemented")
		client.batchedReceiverError = unimplemented
		receiver := newErrorReceiverClient()
		client.receiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(receiver.recvCalls).Should(BeNumerically(">", 1))
	})

	It("closes all connections when the cancel function is called", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		spyClientPool.next = client
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		client.batchedReceiverError = status.Error(codes.Unimplemented, "unimplemented")
		receiver := newSpyReceiverClient()
		receiver.recv = buildLogEnvelope()
		client.receiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		cancel := subscriber.Start(binding)

		// Ensure the context is threaded down.
		Eventually(syslogConnector.connectContext).ShouldNot(BeNil())
		Eventually(client.receiverContext).ShouldNot(BeNil())
		syslogConnectorCtx := syslogConnector.connectContext()
		receiverCtx := client.receiverContext()

		cancel()

		// Ensure the context is now closed.
		Eventually(syslogConnectorCtx.Done).Should(BeClosed())
		Eventually(receiverCtx.Done).Should(BeClosed())
		Eventually(receiver.closeSendCalled).Should(BeTrue())
	})

	It("times out after a configuration duration when opening a stream", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		client.receiveTime = 10 * time.Millisecond
		spyClientPool.next = client
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		client.batchedReceiverError = status.Error(codes.Unimplemented, "unimplemented")
		receiver := newEnvelopeReceiverClient()
		client.receiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(0),
		)

		subscriber.Start(binding)

		// Ensure the context is threaded down.
		Eventually(syslogConnector.connectContext).ShouldNot(BeNil())
		Eventually(client.receiverContext).ShouldNot(BeNil())
		syslogConnectorCtx := syslogConnector.connectContext()
		receiverCtx := client.receiverContext()

		// Ensure the context is now closed.
		Eventually(syslogConnectorCtx.Done).Should(BeClosed())
		Eventually(receiverCtx.Done).Should(BeClosed())
	})

	It("emits ingress metrics", func() {
		spyClientPool := newSpyClientPool()
		spyEmitter := testhelper.NewMetricClient()
		syslogConnector := newSpySyslogConnector()
		writer := newSpyWriter()
		syslogConnector.connect = writer
		client := newSpyLogsProviderClient()
		spyClientPool.next = client
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		client.batchedReceiverError = status.Error(codes.Unimplemented, "unimplemented")
		receiver := newSpyReceiverClient()
		receiver.recv = buildLogEnvelope()
		client.receiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			spyClientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(func() uint64 {
			return spyEmitter.GetDelta("ingress")
		}).Should(Equal(uint64(1)))
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

func newSpyClientPool() *spyClientPool {
	return &spyClientPool{}
}

type spyClientPool struct {
	mu         sync.Mutex
	next       ingress.LogsProviderClient
	nextCalls_ int
}

func (s *spyClientPool) Next() ingress.LogsProviderClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextCalls_++

	return s.next
}

func (s *spyClientPool) nextCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nextCalls_
}

type spySyslogConnector struct {
	mu              sync.Mutex
	connect         egress.Writer
	connectContext_ context.Context
}

func newSpySyslogConnector() *spySyslogConnector {
	return &spySyslogConnector{}
}

func (s *spySyslogConnector) Connect(
	ctx context.Context,
	binding *v1.Binding,
) (egress.Writer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connectContext_ = ctx

	return s.connect, nil
}

func (s *spySyslogConnector) connectContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.connectContext_
}

type spyLogsProviderClient struct {
	mu sync.Mutex

	receiverClient   v2.Egress_ReceiverClient
	receiverContext_ context.Context
	receiverRequest_ *v2.EgressRequest
	receiverError    error
	receiveTime      time.Duration

	batchedReceiverClient   v2.Egress_BatchedReceiverClient
	batchedReceiverRequest_ *v2.EgressBatchRequest
	batchedReceiverError    error
}

func newSpyLogsProviderClient() *spyLogsProviderClient {
	return &spyLogsProviderClient{}
}

func (s *spyLogsProviderClient) Receiver(
	ctx context.Context,
	in *v2.EgressRequest,
	opts ...grpc.CallOption,
) (v2.Egress_ReceiverClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	time.Sleep(s.receiveTime)
	s.receiverContext_ = ctx
	s.receiverRequest_ = in
	return s.receiverClient, s.receiverError
}

func (s *spyLogsProviderClient) receiverContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receiverContext_
}

func (s *spyLogsProviderClient) receiverRequest() *v2.EgressRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receiverRequest_
}

func (s *spyLogsProviderClient) BatchedReceiver(
	ctx context.Context,
	in *v2.EgressBatchRequest,
	opts ...grpc.CallOption,
) (v2.Egress_BatchedReceiverClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.batchedReceiverRequest_ = in

	return s.batchedReceiverClient, s.batchedReceiverError
}

func (s *spyLogsProviderClient) batchedReceiverRequest() *v2.EgressBatchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchedReceiverRequest_
}

func newSpyReceiverClient() *spyReceiverClient {
	return &spyReceiverClient{}
}

type spyReceiverClient struct {
	mu               sync.Mutex
	recv             *v2.Envelope
	done             bool
	closeSendCalled_ bool
	grpc.ClientStream
}

func (s *spyReceiverClient) closeSendCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeSendCalled_
}

func (s *spyReceiverClient) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeSendCalled_ = true

	return nil
}

func (s *spyReceiverClient) Recv() (*v2.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.done {
		s.done = true
		return s.recv, nil
	}

	return nil, errors.New("no more data")
}

func newSpyBatchedReceiverClient() *spyBatchedReceiverClient {
	return &spyBatchedReceiverClient{}
}

type spyBatchedReceiverClient struct {
	mu   sync.Mutex
	recv *v2.EnvelopeBatch
	done bool
	grpc.ClientStream
}

func (s *spyBatchedReceiverClient) CloseSend() error {
	return nil
}

func (s *spyBatchedReceiverClient) Recv() (*v2.EnvelopeBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.done {
		s.done = true
		return s.recv, nil
	}

	return nil, errors.New("no more data")
}

func newEnvelopeReceiverClient() *envelopeReceiver {
	return &envelopeReceiver{}
}

type envelopeReceiver struct {
	grpc.ClientStream
}

func (i *envelopeReceiver) CloseSend() error {
	return nil
}

func (i *envelopeReceiver) Recv() (*v2.Envelope, error) {
	return &v2.Envelope{}, nil
}

func newErrorReceiverClient() *errorReceiver {
	return &errorReceiver{}
}

type errorReceiver struct {
	mu         sync.Mutex
	recvCalls_ int
	grpc.ClientStream
}

func (e *errorReceiver) CloseSend() error {
	return nil
}

func (e *errorReceiver) Recv() (*v2.Envelope, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.recvCalls_++

	return nil, errors.New("never going to work")
}

func (e *errorReceiver) recvCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.recvCalls_
}

func newSpyWriter() *spyWriter {
	return &spyWriter{}
}

type spyWriter struct {
	mu      sync.Mutex
	writes_ int
}

func (s *spyWriter) Write(*v2.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes_ += 1

	return nil
}

func (s *spyWriter) writes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes_
}
