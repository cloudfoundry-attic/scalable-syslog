package egress

import (
	"errors"
	"net/url"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

var (
	errUnsupportedScheme = errors.New("unsupported scheme")
)

// WriteCloser is the interface for all syslog writers.
type WriteCloser interface {
	Write(*loggregator_v2.Envelope) error
	Close() error
}

// WriterBuilder is builder for the various egress syslog writers.
type WriterBuilder struct {
	ioTimeout      time.Duration
	skipCertVerify bool
	tcpOpts        []TCPOption
}

// NewWriterBuilder returns a WriterBuilder configured with BuilderOpt(s).
func NewWriterBuilder(ioTimeout time.Duration, skipCertVerify bool, opts ...BuilderOpt) *WriterBuilder {
	w := &WriterBuilder{
		ioTimeout:      ioTimeout,
		skipCertVerify: skipCertVerify,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Build returns an egress writer based on the scheme of the binding drain
// url.
func (w *WriterBuilder) Build(b *v1.Binding) (WriteCloser, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "syslog":
		return NewTCP(*url, b.AppId, b.Hostname, w.ioTimeout, w.tcpOpts...)
	case "https":
		return NewHTTPS(b, w.skipCertVerify)
	default:
		return nil, errUnsupportedScheme
	}
}

// BuilderOpt provides a means to configure the WriterBuilder.
type BuilderOpt func(*WriterBuilder)

// WithTCPOptions is a BuilderOpt that sets the options
// a TCPWriter will be configured with.
func WithTCPOptions(opts ...TCPOption) BuilderOpt {
	return func(wb *WriterBuilder) {
		wb.tcpOpts = opts
	}
}
