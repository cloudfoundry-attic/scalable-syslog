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

// SyslogConnector creates the various egress syslog writers.
type SyslogConnector struct {
	skipCertVerify bool
	ioTimeout      time.Duration
	dialTimeout    time.Duration
	constructors   map[string]SyslogConstructor
	droppedMetrics map[string]*pulseemitter.CounterMetric
	egressMetrics  map[string]*pulseemitter.CounterMetric
	wg             WaitGroup
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(
	dialTimeout, ioTimeout time.Duration,
	skipCertVerify bool,
	wg WaitGroup,
	opts ...ConnectorOption,
) *SyslogConnector {
	sc := &SyslogConnector{
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
		wg:             wg,
		constructors:   make(map[string]SyslogConstructor),
		droppedMetrics: make(map[string]*pulseemitter.CounterMetric),
		egressMetrics:  make(map[string]*pulseemitter.CounterMetric),
	}
	for _, o := range opts {
		o(sc)
	}
	return sc
}

// SyslogConstructor creates syslog connections to https, syslog, and
// syslog-tls drains
type SyslogConstructor func(
	ctx context.Context,
	binding *v1.Binding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric *pulseemitter.CounterMetric,
) (WriteCloser, error)
type ConnectorOption func(*SyslogConnector)

// WithConstructors allows users to configure the constructors which will
// create syslog network connections. The string key in the constructors map
// should name the protocol.
func WithConstructors(constructors map[string]SyslogConstructor) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.constructors = constructors
	}
}

// WithDroppedMetrics allows users to configure the dropped metrics which will
// be emitted when a syslog writer drops messages
func WithDroppedMetrics(metrics map[string]*pulseemitter.CounterMetric) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.droppedMetrics = metrics
	}
}

// WithEgressMetrics allows users to configure the dropped metrics which will
// be emitted when a syslog writer drops messages
func WithEgressMetrics(metrics map[string]*pulseemitter.CounterMetric) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.egressMetrics = metrics
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
	writer, err := constructor(ctx, b, w.dialTimeout, w.ioTimeout, w.skipCertVerify, egressMetric)
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
