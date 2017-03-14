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
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(dialTimeout, ioTimeout time.Duration, skipCertVerify bool) *SyslogConnector {
	return &SyslogConnector{
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
	}
}

// Connect returns an egress writer based on the scheme of the binding drain
// URL.
func (w *SyslogConnector) Connect(b *v1.Binding) (WriteCloser, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "https":
		return NewHTTPSWriter(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify)
	case "syslog":
		return NewTCPWriter(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify)
	case "syslog-tls":
		return NewTLSWriter(b, w.dialTimeout, w.ioTimeout, w.skipCertVerify)
	default:
		return nil, errors.New("unsupported scheme")
	}
}
