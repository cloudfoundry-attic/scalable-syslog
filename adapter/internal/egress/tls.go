package egress

import (
	"crypto/tls"
	"net"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
)

// TLSWriter represents a syslog writer that connects over unencrypted TCP.
type TLSWriter struct {
	TCPWriter
}

func NewTLSWriter(
	binding *URLBinding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric pulseemitter.CounterMetric,
) WriteCloser {

	dialer := &net.Dialer{
		Timeout: dialTimeout,
	}
	df := func(addr string) (net.Conn, error) {
		return tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
			InsecureSkipVerify: skipCertVerify,
		})
	}

	w := &TLSWriter{
		TCPWriter{
			url:          binding.URL,
			appID:        binding.AppID,
			hostname:     binding.Hostname,
			ioTimeout:    ioTimeout,
			dialFunc:     df,
			scheme:       "syslog-tls",
			egressMetric: egressMetric,
		},
	}

	return w
}
