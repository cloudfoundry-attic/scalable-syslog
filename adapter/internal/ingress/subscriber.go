package ingress

import (
	"log"
	"net/url"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClientPool interface {
	Next() LogsProviderClient
}

type SyslogConnector interface {
	Connect(ctx context.Context, binding *v1.Binding) (w egress.Writer, err error)
}

// LogClient is used to emit logs.
type LogClient interface {
	EmitLog(message string, opts ...loggregator.EmitLogOption)
}

// nullLogClient ensures that the LogClient is in fact optional.
type nullLogClient struct{}

// EmitLog drops all messages into /dev/null.
func (nullLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
}

type SubscriberOption func(s *Subscriber)

func WithStreamOpenTimeout(d time.Duration) SubscriberOption {
	return func(s *Subscriber) {
		s.streamOpenTimeout = d
	}
}

// WithLogClient returns a SubscriberOption that will set up logging for any
// information about a binding.
func WithLogClient(logClient LogClient, sourceIndex string) SubscriberOption {
	return func(s *Subscriber) {
		s.logClient = logClient
		s.sourceIndex = sourceIndex
	}
}

// WithEnableMetricsToSyslog returns a SubscriberOption to override the
// default setting for writing metrics to syslog. By default this feature is
// disabled.
func WithMetricsToSyslogEnabled(enabled bool) SubscriberOption {
	return func(s *Subscriber) {
		s.metricsToSyslogEnabled = enabled
	}
}

// Subscriber streams loggregator egress to the syslog drain.
type Subscriber struct {
	ctx                    context.Context
	pool                   ClientPool
	connector              SyslogConnector
	ingressMetric          pulseemitter.CounterMetric
	logClient              LogClient
	streamOpenTimeout      time.Duration
	sourceIndex            string
	metricsToSyslogEnabled bool
}

type MetricClient interface {
	NewCounterMetric(string, ...pulseemitter.MetricOption) pulseemitter.CounterMetric
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(
	ctx context.Context, // TODO Stop saving the context in the struct.
	p ClientPool,
	c SyslogConnector,
	e MetricClient,
	opts ...SubscriberOption,
) *Subscriber {
	// metric-documentation-v2: (adapter.ingress) Number of envelopes
	// ingressed from RLP.
	ingressMetric := e.NewCounterMetric("ingress",
		pulseemitter.WithVersion(2, 0),
	)

	s := &Subscriber{
		ctx:                    ctx,
		pool:                   p,
		connector:              c,
		ingressMetric:          ingressMetric,
		logClient:              nullLogClient{},
		streamOpenTimeout:      2 * time.Second,
		metricsToSyslogEnabled: false,
	}

	for _, o := range opts {
		o(s)
	}

	return s
}

// Start begins to stream logs from a loggregator egress client to the syslog
// egress writer. Start does not block. Start returns a function that can be
// called to stop streaming logs.
func (s *Subscriber) Start(binding *v1.Binding) func() {
	ctx, cancel := context.WithCancel(s.ctx)

	url, err := url.Parse(binding.Drain)
	if err != nil {
		return cancel
	}

	selectors, ok := s.buildRequestSelectors(binding.AppId, url.Query().Get("drain-type"))
	if !ok {
		s.emitErrorLog(binding.AppId, "Invalid drain-type")
	}

	go s.connectAndRead(ctx, binding, selectors)

	return cancel
}

func (s *Subscriber) connectAndRead(ctx context.Context, binding *v1.Binding, selectors []*v2.Selector) {
	for !isDone(ctx) {
		cont := s.attemptConnectAndRead(ctx, binding, selectors)
		if !cont {
			return
		}
	}
}

func (s *Subscriber) attemptConnectAndRead(ctx context.Context, binding *v1.Binding, selectors []*v2.Selector) bool {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	writer, err := s.connector.Connect(ctx, binding)
	if err != nil {
		log.Printf("Failed connecting to syslog: %s", err)
		return false
	}

	client := s.pool.Next()

	ready := make(chan struct{})
	go func() {
		select {
		case <-time.After(s.streamOpenTimeout):
			cancel()
		case <-ready:
			// Do nothing
		}
	}()

	batchReceiver, err := client.BatchedReceiver(ctx, &v2.EgressBatchRequest{
		ShardId:          buildShardId(binding),
		UsePreferredTags: true,
		Selectors:        selectors,
		LegacySelector:   selectors[0],
	})

	if batchStatus, ok := status.FromError(err); ok {
		if batchStatus.Code() == codes.ResourceExhausted {
			time.Sleep(20 * time.Millisecond)
			return true
		}
	}
	close(ready)
	if err != nil {
		client.Invalidate()
		return true
	}
	defer batchReceiver.CloseSend()

	if err := s.batchReadWriteLoop(binding.AppId, batchReceiver, writer); err != nil {
		loopStatus, ok := status.FromError(err)
		if ok && loopStatus.Code() == codes.ResourceExhausted {
			time.Sleep(20 * time.Millisecond)
			return true
		}

		client.Invalidate()
		if ok && (loopStatus.Code() == codes.Canceled || loopStatus.Code() == codes.Unavailable) {
			return true
		}
		log.Printf("Subscriber read/write loop has unexpectedly closed: %s", err)
	}

	return true
}

func (s *Subscriber) readWriteLoop(sourceId string, r v2.Egress_ReceiverClient, w egress.Writer) error {
	for {
		env, err := r.Recv()
		if err != nil {
			return err
		}

		// Ensure this is an envelope we actually want and we don't accidently
		// write envelopes that were not meant for this binding. This is
		// defensive and hopefully the logs provider will do its job, but
		// better safe than sorry.
		if env.GetSourceId() != sourceId {
			log.Print("Warning! Logs provider gave us an unexpected app-id!")
			continue
		}

		s.ingressMetric.Increment(1)
		// We decided to ignore the error from the writer since in most
		// situations the connector will provide a diode writer and the diode
		// writer never returns an error.
		_ = w.Write(env)
	}
}

func (s *Subscriber) batchReadWriteLoop(sourceId string, r v2.Egress_BatchedReceiverClient, w egress.Writer) error {
	for {
		envBatch, err := r.Recv()
		if err != nil {
			return err
		}

		s.ingressMetric.Increment(uint64(len(envBatch.Batch)))

		for _, env := range envBatch.Batch {

			// Ensure this is an envelope we actually want and we don't accidently
			// write envelopes that were not meant for this binding. This is
			// defensive and hopefully the logs provider will do its job, but
			// better safe than sorry.
			if env.GetSourceId() != sourceId {
				log.Print("Warning! Logs provider gave us an unexpected app-id!")
				continue
			}
			// We decided to ignore the error from the writer since in most
			// situations the connector will provide a diode writer and the diode
			// writer never returns an error.
			_ = w.Write(env)
		}
	}
}

func (s *Subscriber) emitErrorLog(appID, message string) {
	option := loggregator.WithAppInfo(
		appID,
		"SYS",
		s.sourceIndex,
	)
	s.logClient.EmitLog(message, option)
}

func (s *Subscriber) buildRequestSelectors(appID, drainType string) ([]*v2.Selector, bool) {
	if !s.metricsToSyslogEnabled {
		return []*v2.Selector{
			{
				SourceId: appID,
				Message: &v2.Selector_Log{
					Log: &v2.LogSelector{},
				},
			},
		}, true
	}

	switch drainType {
	case "", "logs":
		return []*v2.Selector{
			{
				SourceId: appID,
				Message: &v2.Selector_Log{
					Log: &v2.LogSelector{},
				},
			},
		}, true
	case "metrics":
		return []*v2.Selector{
			{
				SourceId: appID,
				Message: &v2.Selector_Gauge{
					Gauge: &v2.GaugeSelector{},
				},
			},
			{
				SourceId: appID,
				Message: &v2.Selector_Counter{
					Counter: &v2.CounterSelector{},
				},
			},
		}, true
	case "all":
		return []*v2.Selector{
			{
				SourceId: appID,
				Message: &v2.Selector_Log{
					Log: &v2.LogSelector{},
				},
			},
			{
				SourceId: appID,
				Message: &v2.Selector_Gauge{
					Gauge: &v2.GaugeSelector{},
				},
			},
			{
				SourceId: appID,
				Message: &v2.Selector_Counter{
					Counter: &v2.CounterSelector{},
				},
			},
		}, true
	default:
		return []*v2.Selector{
			{
				SourceId: appID,
				Message: &v2.Selector_Log{
					Log: &v2.LogSelector{},
				},
			},
		}, false
	}
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func buildShardId(binding *v1.Binding) (key string) {
	return binding.AppId + binding.Hostname + binding.Drain
}
