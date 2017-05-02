package egress

import (
	"errors"
	"log"
	"net/url"
	"time"

	"code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"
	"github.com/cloudfoundry/diodes"
)

// WriteCloser is the interface for all syslog writers.
type WriteCloser interface {
	Write(*loggregator_v2.Envelope) error
	Close() error
}

// SyslogConnector creates the various egress syslog writers.
type SyslogConnector struct {
	skipCertVerify bool
	ioTimeout      time.Duration
	dialTimeout    time.Duration
	metricClient   metricemitter.MetricClient
	constructors   map[string]SyslogConstructor
	droppedMetrics map[string]*metricemitter.CounterMetric
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(
	dialTimeout, ioTimeout time.Duration,
	skipCertVerify bool,
	metricClient metricemitter.MetricClient,
	opts ...ConnectorOption,
) *SyslogConnector {
	httpsDroppedMetric := metricClient.NewCounterMetric("dropped",
		metricemitter.WithVersion(2, 0),
		metricemitter.WithTags(map[string]string{"drain-protocol": "https"}),
	)

	syslogDroppedMetric := metricClient.NewCounterMetric("dropped",
		metricemitter.WithVersion(2, 0),
		metricemitter.WithTags(map[string]string{"drain-protocol": "syslog"}),
	)

	syslogTLSDroppedMetric := metricClient.NewCounterMetric("dropped",
		metricemitter.WithVersion(2, 0),
		metricemitter.WithTags(map[string]string{"drain-protocol": "syslog-tls"}),
	)

	sc := &SyslogConnector{
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
		metricClient:   metricClient,
		constructors: map[string]SyslogConstructor{
			"https":      NewHTTPSWriter,
			"syslog":     NewTCPWriter,
			"syslog-tls": NewTLSWriter,
		},
		droppedMetrics: map[string]*metricemitter.CounterMetric{
			"https":      httpsDroppedMetric,
			"syslog":     syslogDroppedMetric,
			"syslog-tls": syslogTLSDroppedMetric,
		},
	}
	for _, o := range opts {
		o(sc)
	}
	return sc
}

type SyslogConstructor func(*v1.Binding, time.Duration, time.Duration, bool, metricemitter.MetricClient) (WriteCloser, error)
type ConnectorOption func(*SyslogConnector)

func WithConstructors(constructors map[string]SyslogConstructor) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.constructors = constructors
	}
}

func WithDroppedMetrics(metrics map[string]*metricemitter.CounterMetric) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.droppedMetrics = metrics
	}
}

// Connect returns an egress writer based on the scheme of the binding drain
// URL.
func (w *SyslogConnector) Connect(b *v1.Binding) (WriteCloser, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	droppedMetric := w.droppedMetrics[url.Scheme]
	constructor, ok := w.constructors[url.Scheme]
	if !ok {
		return nil, errors.New("unsupported scheme")
	}
	writer, err := constructor(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify, w.metricClient)
	if err != nil {
		return nil, err
	}

	dw := NewDiodeWriter(writer, diodes.AlertFunc(func(missed int) {
		if droppedMetric != nil {
			droppedMetric.Increment(uint64(missed))
		}

		log.Printf("Dropped %d %s logs", missed, url.Scheme)
	}))

	return dw, nil
}
