package egress

import (
	"errors"
	"net/url"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
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
	alerter        Alerter
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(dialTimeout, ioTimeout time.Duration, skipCertVerify bool, opts ...ConnectorOption) *SyslogConnector {
	sc := &SyslogConnector{
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
		constructors: map[string]SyslogConstructor{
			"https":      NewHTTPSWriter,
			"syslog":     NewTCPWriter,
			"syslog-tls": NewTLSWriter,
		},
		alerter: NoopAlerter{},
	}
	for _, o := range opts {
		o(sc)
	}
	return sc
}

type SyslogConstructor func(*v1.Binding, time.Duration, time.Duration, bool) (WriteCloser, error)

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
	writer, err := constructor(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify)
	if err != nil {
		return nil, err
	}
	dw := NewDiodeWriter(writer, w.alerter)
	return dw, nil
}

type NoopAlerter struct{}

func (NoopAlerter) Alert(missed int) {}
