package ingress

import (
	"log"
	"time"

	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	v2 "code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"
	"golang.org/x/net/context"
)

type ClientPool interface {
	Next() (client v2.EgressClient)
}

type SyslogConnector interface {
	Connect(ctx context.Context, binding *v1.Binding) (w egress.Writer, err error)
}

type SubscriberOption func(s *Subscriber)

func WithStreamOpenTimeout(d time.Duration) SubscriberOption {
	return func(s *Subscriber) {
		s.streamOpenTimeout = d
	}
}

// Subscriber streams loggregator egress to the syslog drain.
type Subscriber struct {
	ctx               context.Context
	pool              ClientPool
	connector         SyslogConnector
	ingressMetric     *metricemitter.CounterMetric
	streamOpenTimeout time.Duration
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(
	ctx context.Context,
	p ClientPool,
	c SyslogConnector,
	e metricemitter.MetricClient,
	opts ...SubscriberOption,
) *Subscriber {
	// metric-documentation-v2: (adapter.ingress) Number of envelopes
	// ingressed from RLP.
	ingressMetric := e.NewCounterMetric("ingress",
		metricemitter.WithVersion(2, 0),
	)

	s := &Subscriber{
		ctx:               ctx,
		pool:              p,
		connector:         c,
		ingressMetric:     ingressMetric,
		streamOpenTimeout: 5 * time.Second,
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

	go s.connectAndRead(ctx, binding)

	return cancel
}

func (s *Subscriber) connectAndRead(ctx context.Context, binding *v1.Binding) {
	for !isDone(ctx) {
		cont := s.attemptConnectAndRead(ctx, binding)
		if !cont {
			return
		}
	}
}

func (s *Subscriber) attemptConnectAndRead(ctx context.Context, binding *v1.Binding) bool {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	writer, err := s.connector.Connect(ctx, binding)
	if err != nil {
		log.Println("Failed connecting to syslog: %s", err)
		// If connect fails it is likely due to a parse error with the binding
		// URL or other input error. As a result we should not retry
		// connecting.
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

	receiver, err := client.Receiver(ctx, &v2.EgressRequest{
		ShardId: buildShardId(binding),
		Filter: &v2.Filter{
			SourceId: binding.AppId,
			Message: &v2.Filter_Log{
				Log: &v2.LogFilter{},
			},
		},
	})
	close(ready)
	if err != nil {
		log.Printf("failed to open stream for binding %s: %s", binding.AppId, err)
		return true
	}
	defer receiver.CloseSend()

	err = s.readWriteLoop(receiver, writer)
	log.Printf("Subscriber read/write loop has unexpectedly closed: %s", err)

	return true
}

func (s *Subscriber) readWriteLoop(r v2.Egress_ReceiverClient, w egress.Writer) error {
	for {
		env, err := r.Recv()
		if err != nil {
			return err
		}
		if env.GetLog() == nil {
			continue
		}

		s.ingressMetric.Increment(1)
		// We decided to ignore the error from the writer since in most
		// situations the connector will provide a diode writer and the diode
		// writer never returns an error.
		_ = w.Write(env)
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
