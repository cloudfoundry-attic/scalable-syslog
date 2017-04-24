package egress

import (
	"errors"
	"log"
	"net/url"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/metric"
	gendiodes "github.com/cloudfoundry/diodes"
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
	constructors   map[string]SyslogConstructor
	alerter        gendiodes.Alerter
	emitter        MetricEmitter
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(dialTimeout, ioTimeout time.Duration, skipCertVerify bool, emitter MetricEmitter, opts ...ConnectorOption) *SyslogConnector {
	sc := &SyslogConnector{
		emitter:        emitter,
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
		constructors: map[string]SyslogConstructor{
			"https":      NewHTTPSWriter,
			"syslog":     NewTCPWriter,
			"syslog-tls": NewTLSWriter,
		},
		alerter: logAlerter{},
	}
	for _, o := range opts {
		o(sc)
	}
	return sc
}

type MetricEmitter interface {
	IncCounter(name string, options ...metric.IncrementOpt)
}

type SyslogConstructor func(*v1.Binding, time.Duration, time.Duration, bool, MetricEmitter) (WriteCloser, error)

type ConnectorOption func(*SyslogConnector)

func WithConstructors(constructors map[string]SyslogConstructor) func(*SyslogConnector) {
	return func(sc *SyslogConnector) {
		sc.constructors = constructors
	}
}

// Connect returns an egress writer based on the scheme of the binding drain
// URL.
func (w *SyslogConnector) Connect(b *v1.Binding) (WriteCloser, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	constructor, ok := w.constructors[url.Scheme]
	if !ok {
		return nil, errors.New("unsupported scheme")
	}
	writer, err := constructor(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify, w.emitter)
	if err != nil {
		return nil, err
	}
	dw := NewDiodeWriter(writer, w.alerter)
	return dw, nil
}

type logAlerter struct{}

func (logAlerter) Alert(missed int) {
	log.Printf("Dropped %d logs", missed)
}
