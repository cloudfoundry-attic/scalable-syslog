package egress

import (
	"crypto/tls"
	"net"
	"net/url"
	"time"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"
)

// TLSWriter represents a syslog writer that connects over unencrypted TCP.
type TLSWriter struct {
	TCPWriter
}

func NewTLSWriter(
	binding *v1.Binding,
	dialTimeout, ioTimeout time.Duration,
	skipCertVerify bool,
	metricClient metricemitter.MetricClient,
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
			url:       drainURL,
			appID:     binding.AppId,
			hostname:  binding.Hostname,
			ioTimeout: ioTimeout,
			dialFunc:  df,
			scheme:    "syslog-tls",
		},
	}

	w.egressMetric = metricClient.NewCounterMetric(
		"egress",
		metricemitter.WithVersion(2, 0),
		metricemitter.WithTags(map[string]string{"drain-protocol": w.scheme}),
	)

	go w.connect()

	return w, nil
}
