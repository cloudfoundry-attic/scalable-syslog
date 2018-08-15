package ingress_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
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
	var (
		clientPool            *spyClientPool
		spyEmitter            *testhelper.SpyMetricClient
		syslogConnector       *spySyslogConnector
		writer                *spyWriter
		client                *spyLogsProviderClient
		logClient             *spyLogClient
		batchedReceiverClient *spyBatchedReceiverClient
		binding               *v1.Binding
	)

	BeforeEach(func() {
		clientPool = newSpyClientPool()
		spyEmitter = testhelper.NewMetricClient()
		syslogConnector = newSpySyslogConnector()
		writer = newSpyWriter()
		client = newSpyLogsProviderClient()
		logClient = newSpyLogClient()
		batchedReceiverClient = newSpyBatchedReceiverClient()
		binding = &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}

		syslogConnector.connect = writer
		clientPool.next = client
	})

	It("opens a stream with a batching egress client", func() {
		batchedReceiverClient.recv = buildBatchedLogs(3)
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
			ingress.WithLogClient(logClient, "123"),
		)

		subscriber.Start(binding)

		Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())
		Expect(client.batchedReceiverRequest().Selectors[0].GetSourceId()).To(Equal(binding.AppId))
		Expect(client.batchedReceiverRequest().Selectors[0].GetLog()).ToNot(BeNil())
		Expect(client.batchedReceiverRequest().ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))
		Expect(client.batchedReceiverRequest().UsePreferredTags).To(BeTrue())
		Eventually(writer.writes).Should(Equal(3))
		Expect(logClient.message()).To(BeEmpty())
	})

	It("acquires another client when one client fails", func() {
		client.batchedReceiverError = errors.New("cannot get batcher")
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(clientPool.nextCalls).Should(BeNumerically(">", 1))
	})

	It("acquires another receiver when Recv() fails", func() {
		receiver := newErrorReceiverClient()
		client.batchedReceiverClient = receiver
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(receiver.recvCalls).Should(BeNumerically(">=", 1))
	})

	It("does not invalidate the client if BatchedReceiver fails with ResourceExhausted code", func() {
		batchedReceiverClient.recvErr = status.Error(codes.ResourceExhausted, "resource exhausted")
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Consistently(client.invalidated).Should(Equal(false))
	})

	It("invalidates the client when BatchedReceiver fails", func() {
		client.batchedReceiverError = errors.New("cannot get batcher")
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(client.invalidated).Should(Equal(true))
	})

	It("invalidates the client when Batched's Recv fails", func() {
		batchedReceiverClient := newSpyBatchedReceiverClient()
		batchedReceiverClient.done = true // This will cause Recv() to return an error
		client.batchedReceiverClient = batchedReceiverClient
		binding := &v1.Binding{
			AppId:    "some-app-id",
			Hostname: "some-host-name",
			Drain:    "some-drain",
		}
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(client.invalidated).Should(Equal(true))
	})

	It("closes all connections when the cancel function is called", func() {
		batchedReceiverClient.recv = buildBatchedLogs(3)
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		cancel := subscriber.Start(binding)

		// Ensure the context is threaded down.
		Eventually(syslogConnector.connectContext).ShouldNot(BeNil())
		Eventually(client.batchedReceiverContext).ShouldNot(BeNil())
		syslogConnectorCtx := syslogConnector.connectContext()
		receiverCtx := client.batchedReceiverContext()

		cancel()

		// Ensure the context is now closed.
		Eventually(syslogConnectorCtx.Done).Should(BeClosed())
		Eventually(receiverCtx.Done).Should(BeClosed())
		Eventually(batchedReceiverClient.closeSendCalled).Should(BeTrue())
	})

	It("times out after a configuration duration when opening a stream", func() {
		batchedReceiverClient.recv = buildBatchedLogs(3)
		client.batchedReceiveTime = 10 * time.Millisecond
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(0),
		)

		subscriber.Start(binding)

		// Ensure the context is threaded down.
		Eventually(syslogConnector.connectContext).ShouldNot(BeNil())
		Eventually(client.batchedReceiverContext).ShouldNot(BeNil())
		syslogConnectorCtx := syslogConnector.connectContext()
		receiverCtx := client.batchedReceiverContext()

		// Ensure the context is now closed.
		Eventually(syslogConnectorCtx.Done).Should(BeClosed())
		Eventually(receiverCtx.Done).Should(BeClosed())
	})

	It("emits ingress metrics", func() {
		batchedReceiverClient.recv = buildBatchedLogs(1)
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Eventually(spyEmitter.GetMetric("ingress").Delta).Should(Equal(uint64(1)))
	})

	It("ignores batched received envelopes with an unexpected app-id", func() {
		batchedReceiverClient.recv = &v2.EnvelopeBatch{
			Batch: []*v2.Envelope{buildLogEnvelope("invalid-source-id")},
		}
		client.batchedReceiverClient = batchedReceiverClient
		subscriber := ingress.NewSubscriber(
			context.TODO(),
			clientPool,
			syslogConnector,
			spyEmitter,
			ingress.WithStreamOpenTimeout(500*time.Millisecond),
		)

		subscriber.Start(binding)

		Consistently(writer.writes).Should(BeZero())
	})

	Describe("drain-type option", func() {
		BeforeEach(func() {
			batchedReceiverClient.recv = buildBatchedLogs(3)
			client.batchedReceiverClient = batchedReceiverClient
		})

		Context("when metrics-to-syslog is disabled", func() {
			It("ignores drain-type", func() {
				subscriber := ingress.NewSubscriber(
					context.TODO(),
					clientPool,
					syslogConnector,
					spyEmitter,
					ingress.WithStreamOpenTimeout(500*time.Millisecond),
					ingress.WithMetricsToSyslogEnabled(false),
				)

				binding := &v1.Binding{
					AppId:    "some-app-id",
					Hostname: "some-host-name",
					Drain:    "https://some-drain?drain-type=metrics",
				}
				subscriber.Start(binding)

				Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())

				req := client.batchedReceiverRequest()
				Expect(req.GetSelectors()).To(HaveLen(1))

				selector := req.GetSelectors()[0]
				Expect(selector.GetLog()).ToNot(BeNil())
				Expect(req.GetLegacySelector().GetLog()).ToNot(BeNil())
			})
		})

		Context("when drain-type is empty", func() {
			It("requests only logs", func() {
				subscriber := ingress.NewSubscriber(
					context.TODO(),
					clientPool,
					syslogConnector,
					spyEmitter,
					ingress.WithStreamOpenTimeout(500*time.Millisecond),
					ingress.WithMetricsToSyslogEnabled(true),
				)

				binding := &v1.Binding{
					AppId:    "some-app-id",
					Hostname: "some-host-name",
					Drain:    "https://some-drain",
				}
				subscriber.Start(binding)

				Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())

				req := client.batchedReceiverRequest()
				Expect(req.GetSelectors()).To(HaveLen(1))

				selector := req.GetSelectors()[0]
				Expect(selector.GetLog()).ToNot(BeNil())
				Expect(req.GetLegacySelector().GetLog()).ToNot(BeNil())
			})
		})

		Context("when drain-type is logs", func() {
			It("requests only logs", func() {
				subscriber := ingress.NewSubscriber(
					context.TODO(),
					clientPool,
					syslogConnector,
					spyEmitter,
					ingress.WithStreamOpenTimeout(500*time.Millisecond),
					ingress.WithMetricsToSyslogEnabled(true),
				)

				binding := &v1.Binding{
					AppId:    "some-app-id",
					Hostname: "some-host-name",
					Drain:    "https://some-drain?drain-type=logs",
				}
				subscriber.Start(binding)

				Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())

				req := client.batchedReceiverRequest()
				Expect(req.GetSelectors()).To(HaveLen(1))

				selector := req.GetSelectors()[0]
				Expect(selector.GetLog()).ToNot(BeNil())
				Expect(req.GetLegacySelector().GetLog()).ToNot(BeNil())
			})
		})

		Context("when drain-type is metrics", func() {
			It("requests only gauge metrics", func() {
				subscriber := ingress.NewSubscriber(
					context.TODO(),
					clientPool,
					syslogConnector,
					spyEmitter,
					ingress.WithStreamOpenTimeout(500*time.Millisecond),
					ingress.WithMetricsToSyslogEnabled(true),
				)

				binding := &v1.Binding{
					AppId:    "some-app-id",
					Hostname: "some-host-name",
					Drain:    "https://some-drain?drain-type=metrics",
				}
				subscriber.Start(binding)

				Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())

				req := client.batchedReceiverRequest()
				Expect(req.GetSelectors()).To(HaveLen(2))

				selector := req.GetSelectors()[0]
				Expect(selector.GetGauge()).ToNot(BeNil())

				selector = req.GetSelectors()[1]
				Expect(selector.GetCounter()).ToNot(BeNil())
			})
		})

		Context("when drain-type is all", func() {
			It("requests logs and gauge metrics", func() {
				subscriber := ingress.NewSubscriber(
					context.TODO(),
					clientPool,
					syslogConnector,
					spyEmitter,
					ingress.WithStreamOpenTimeout(500*time.Millisecond),
					ingress.WithMetricsToSyslogEnabled(true),
				)

				binding := &v1.Binding{
					AppId:    "some-app-id",
					Hostname: "some-host-name",
					Drain:    "https://some-drain?drain-type=all",
				}
				subscriber.Start(binding)

				Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())

				req := client.batchedReceiverRequest()
				Expect(req.GetSelectors()).To(HaveLen(3))

				Expect(req.GetSelectors()[0].GetLog()).ToNot(BeNil())
				Expect(req.GetSelectors()[1].GetGauge()).ToNot(BeNil())
				Expect(req.GetSelectors()[2].GetCounter()).ToNot(BeNil())
				Expect(req.GetLegacySelector().GetLog()).ToNot(BeNil())
			})
		})

		It("emits a log to the logstream on invalid drain-type", func() {
			subscriber := ingress.NewSubscriber(
				context.TODO(),
				clientPool,
				syslogConnector,
				spyEmitter,
				ingress.WithStreamOpenTimeout(500*time.Millisecond),
				ingress.WithLogClient(logClient, "some-source-index"),
				ingress.WithMetricsToSyslogEnabled(true),
			)

			binding := &v1.Binding{
				AppId:    "some-app-id",
				Hostname: "some-host-name",
				Drain:    "https://some-drain?drain-type=false-drain",
			}
			subscriber.Start(binding)

			Eventually(client.batchedReceiverRequest).ShouldNot(BeNil())
			Expect(client.batchedReceiverRequest().GetLegacySelector().GetLog()).ToNot(BeNil())
			Expect(client.batchedReceiverRequest().Selectors[0].GetSourceId()).To(Equal(binding.AppId))
			Expect(client.batchedReceiverRequest().Selectors[0].GetLog()).ToNot(BeNil())
			Expect(client.batchedReceiverRequest().ShardId).To(Equal(fmt.Sprint(binding.AppId, binding.Hostname, binding.Drain)))
			Expect(client.batchedReceiverRequest().UsePreferredTags).To(BeTrue())
			Eventually(writer.writes).Should(Equal(3))

			Expect(logClient.message()).To(ContainElement("Invalid drain-type"))
			Expect(logClient.appID()).To(ContainElement("some-app-id"))
			Expect(logClient.sourceType()).To(HaveKey("SYS"))
			Expect(logClient.sourceInstance()).To(HaveKey("some-source-index"))
		})
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

func buildLogEnvelope(sourceID string) *v2.Envelope {
	return &v2.Envelope{
		Tags: map[string]string{
			"source_type":     "APP",
			"source_instance": "2",
		},
		Timestamp: 12345678,
		SourceId:  sourceID,
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
		env := buildLogEnvelope("some-app-id")
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

	batchedReceiverClient   v2.Egress_BatchedReceiverClient
	batchedReceiverRequest_ *v2.EgressBatchRequest
	batchedReceiverError    error
	batchedReceiverContext_ context.Context
	batchedReceiveTime      time.Duration

	invalid bool
}

func newSpyLogsProviderClient() *spyLogsProviderClient {
	return &spyLogsProviderClient{}
}

func (s *spyLogsProviderClient) invalidated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalid
}

func (s *spyLogsProviderClient) Valid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalid
}

func (s *spyLogsProviderClient) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalid = true
}

func (s *spyLogsProviderClient) BatchedReceiver(
	ctx context.Context,
	in *v2.EgressBatchRequest,
	opts ...grpc.CallOption,
) (v2.Egress_BatchedReceiverClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	time.Sleep(s.batchedReceiveTime)

	s.batchedReceiverRequest_ = in
	s.batchedReceiverContext_ = ctx

	return s.batchedReceiverClient, s.batchedReceiverError
}

func (s *spyLogsProviderClient) batchedReceiverRequest() *v2.EgressBatchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchedReceiverRequest_
}

func (s *spyLogsProviderClient) batchedReceiverContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.batchedReceiverContext_
}

func newSpyBatchedReceiverClient() *spyBatchedReceiverClient {
	return &spyBatchedReceiverClient{}
}

type spyBatchedReceiverClient struct {
	mu      sync.Mutex
	recv    *v2.EnvelopeBatch
	recvErr error
	done    bool
	grpc.ClientStream
	closeSendCalled_ bool
}

func (s *spyBatchedReceiverClient) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeSendCalled_ = true

	return nil
}

func (s *spyBatchedReceiverClient) Recv() (*v2.EnvelopeBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.recvErr != nil {
		return nil, s.recvErr
	}

	if !s.done {
		s.done = true
		return s.recv, nil
	}

	return nil, errors.New("no more data")
}

func (s *spyBatchedReceiverClient) closeSendCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeSendCalled_
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

type errorReceiver struct {
	mu         sync.Mutex
	recvCalls_ int
	grpc.ClientStream
}

func newErrorReceiverClient() *errorReceiver {
	return &errorReceiver{}
}

func (e *errorReceiver) CloseSend() error {
	return nil
}

func (e *errorReceiver) Recv() (*v2.EnvelopeBatch, error) {
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

type spyLogClient struct {
	mu       sync.Mutex
	_message []string
	_appID   []string

	// We use maps to ensure that we can query the keys
	_sourceType     map[string]struct{}
	_sourceInstance map[string]struct{}
}

func newSpyLogClient() *spyLogClient {
	return &spyLogClient{
		_sourceType:     make(map[string]struct{}),
		_sourceInstance: make(map[string]struct{}),
	}
}

func (s *spyLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
	s.mu.Lock()
	defer s.mu.Unlock()

	env := &v2.Envelope{
		Tags: make(map[string]string),
	}

	for _, o := range opts {
		o(env)
	}

	s._message = append(s._message, message)
	s._appID = append(s._appID, env.SourceId)
	s._sourceType[env.GetTags()["source_type"]] = struct{}{}
	s._sourceInstance[env.GetInstanceId()] = struct{}{}
}

func (s *spyLogClient) message() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s._message
}

func (s *spyLogClient) appID() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s._appID
}

func (s *spyLogClient) sourceType() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Copy map so the orig does not escape the mutex and induce a race.
	m := make(map[string]struct{})
	for k := range s._sourceType {
		m[k] = struct{}{}
	}

	return m
}

func (s *spyLogClient) sourceInstance() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Copy map so the orig does not escape the mutex and induce a race.
	m := make(map[string]struct{})
	for k := range s._sourceInstance {
		m[k] = struct{}{}
	}

	return m
}
