package egress

import (
	"errors"
	"io"
	"log"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"github.com/cloudfoundry/diodes"
)

// Write is the interface for all diode writers.
type Writer interface {
	Write(*loggregator_v2.Envelope) error
}

// WriteCloser is the interface for all syslog writers.
type WriteCloser interface {
	Writer
	io.Closer
}

type MetricClient interface {
	NewCounterMetric(string, ...pulseemitter.MetricOption) *pulseemitter.CounterMetric
}

// SyslogConnector creates the various egress syslog writers.
type SyslogConnector struct {
	skipCertVerify bool
	ioTimeout      time.Duration
	dialTimeout    time.Duration
	metricClient   MetricClient
	constructors   map[string]SyslogConstructor
	droppedMetrics map[string]*pulseemitter.CounterMetric
	egressMetrics  map[string]*pulseemitter.CounterMetric
	wg             WaitGroup
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(
	dialTimeout, ioTimeout time.Duration,
	skipCertVerify bool,
	metricClient MetricClient,
	wg WaitGroup,
	opts ...ConnectorOption,
) *SyslogConnector {
	// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
	// when sending to a syslog drain over https.
	httpsDroppedMetric := metricClient.NewCounterMetric("dropped",
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": "https"}),
	)
	// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
	// to a syslog drain over https.
	httpsEgressMetric := metricClient.NewCounterMetric("egress",
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": "https"}),
	)

	// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
	// when sending to a syslog drain over syslog.
	syslogDroppedMetric := metricClient.NewCounterMetric("dropped",
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": "syslog"}),
	)
	// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
	// to a syslog drain over syslog.
	syslogEgressMetric := metricClient.NewCounterMetric("egress",
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": "syslog"}),
	)

	// metric-documentation-v2: (adapter.dropped) Number of envelopes dropped
	// when sending to a syslog drain over syslog-tls.
	syslogTLSDroppedMetric := metricClient.NewCounterMetric("dropped",
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": "syslog-tls"}),
	)
	// metric-documentation-v2: (adapter.egress) Number of envelopes sent out
	// to a syslog drain over syslog-tls.
	syslogTLSEgressMetric := metricClient.NewCounterMetric("egress",
		pulseemitter.WithVersion(2, 0),
		pulseemitter.WithTags(map[string]string{"drain-protocol": "syslog-tls"}),
	)

	sc := &SyslogConnector{
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
		metricClient:   metricClient,
		wg:             wg,
		constructors: map[string]SyslogConstructor{
			"https":      NewHTTPSWriter,
			"syslog":     NewTCPWriter,
			"syslog-tls": NewTLSWriter,
		},
		droppedMetrics: map[string]*pulseemitter.CounterMetric{
			"https":      httpsDroppedMetric,
			"syslog":     syslogDroppedMetric,
			"syslog-tls": syslogTLSDroppedMetric,
		},
		egressMetrics: map[string]*pulseemitter.CounterMetric{
			"https":      httpsEgressMetric,
			"syslog":     syslogEgressMetric,
			"syslog-tls": syslogTLSEgressMetric,
		},
	}
	for _, o := range opts {
		o(sc)
	}
	return sc
}

type SyslogConstructor func(*v1.Binding, time.Duration, time.Duration, bool, *pulseemitter.CounterMetric) (WriteCloser, error)
type ConnectorOption func(*SyslogConnector)

func WithConstructors(constructors map[string]SyslogConstructor) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.constructors = constructors
	}
}

func WithDroppedMetrics(metrics map[string]*pulseemitter.CounterMetric) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.droppedMetrics = metrics
	}
}

// Connect returns an egress writer based on the scheme of the binding drain
// URL.
func (w *SyslogConnector) Connect(ctx context.Context, b *v1.Binding) (Writer, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	droppedMetric := w.droppedMetrics[url.Scheme]
	egressMetric := w.egressMetrics[url.Scheme]
	constructor, ok := w.constructors[url.Scheme]
	if !ok {
		return nil, errors.New("unsupported scheme")
	}
	writer, err := constructor(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify, egressMetric)
	if err != nil {
		return nil, err
	}

	dw := NewDiodeWriter(ctx, writer, diodes.AlertFunc(func(missed int) {
		if droppedMetric != nil {
			droppedMetric.Increment(uint64(missed))
		}

		log.Printf("Dropped %d %s logs", missed, url.Scheme)
	}), w.wg)

	return dw, nil
}
