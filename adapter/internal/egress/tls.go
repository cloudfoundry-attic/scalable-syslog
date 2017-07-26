package egress

import (
	"crypto/tls"
	"net"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

// TLSWriter represents a syslog writer that connects over unencrypted TCP.
type TLSWriter struct {
	TCPWriter
}

func NewTLSWriter(
	ctx context.Context,
	binding *v1.Binding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric *pulseemitter.CounterMetric,
) (WriteCloser, error) {
	drainURL, err := url.Parse(binding.Drain)
	// TODO: remove parsing/error from here
	if err != nil {
		return nil, err
	}

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
			url:          drainURL,
			appID:        binding.AppId,
			hostname:     binding.Hostname,
			ioTimeout:    ioTimeout,
			dialFunc:     df,
			scheme:       "syslog-tls",
			egressMetric: egressMetric,
			ctx:          ctx,
		},
	}

	return w, nil
}
