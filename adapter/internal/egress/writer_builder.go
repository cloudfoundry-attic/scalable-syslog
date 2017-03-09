package egress

import (
	"crypto/tls"
	"errors"
	"net"
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
	skipCertVerify bool
	ioTimeout      time.Duration
	dialer         *net.Dialer
}

// NewWriterBuilder configures and returns a pointer to a new WriterBuilder.
func NewWriterBuilder(dialTimeout, ioTimeout time.Duration, skipCertVerify bool) *WriterBuilder {
	return &WriterBuilder{
		skipCertVerify: skipCertVerify,
		ioTimeout:      ioTimeout,
		dialer: &net.Dialer{
			Timeout: dialTimeout,
		},
	}
}

// Build returns an egress writer based on the scheme of the binding drain url.
func (w *WriterBuilder) Build(b *v1.Binding) (WriteCloser, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "https":
		return NewHTTPS(b, w.skipCertVerify)
	case "syslog":
		df := func(addr string) (net.Conn, error) {
			return w.dialer.Dial("tcp", addr)
		}
		return NewTCPWriter(b, w.ioTimeout, WithDialFunc(df))
	case "syslog-tls":
		df := func(addr string) (net.Conn, error) {
			return tls.DialWithDialer(w.dialer, "tcp", addr, &tls.Config{
				InsecureSkipVerify: w.skipCertVerify,
			})
		}
		return NewTCPWriter(b, w.ioTimeout, WithDialFunc(df))
	default:
		return nil, errUnsupportedScheme
	}
}
