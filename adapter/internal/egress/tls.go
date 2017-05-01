package egress

import (
	"crypto/tls"
	"net"
	"net/url"
	"time"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

// TLSWriter represents a syslog writer that connects over unencrypted TCP.
type TLSWriter struct {
	TCPWriter
}

func NewTLSWriter(binding *v1.Binding, dialTimeout, ioTimeout time.Duration, skipCertVerify bool, emitter MetricEmitter) (WriteCloser, error) {
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
			emitter:        emitter,
			url:            drainURL,
			appID:          binding.AppId,
			hostname:       binding.Hostname,
			ioTimeout:      ioTimeout,
			dialFunc:       df,
			metricThrottle: &metricThrottler{},
			scheme:         "syslog-tls",
		},
	}
	go w.connect()

	return w, nil
}
