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

// NetworkTimeoutConfig stores various timeout values.
type NetworkTimeoutConfig struct {
	Keepalive    time.Duration
	DialTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewTLSWriter(
	binding *URLBinding,
	netConf NetworkTimeoutConfig,
	skipCertVerify bool,
	egressMetric pulseemitter.CounterMetric,
) WriteCloser {

	dialer := &net.Dialer{
		Timeout:   netConf.DialTimeout,
		KeepAlive: netConf.Keepalive,
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
			writeTimeout: netConf.WriteTimeout,
			dialFunc:     df,
			scheme:       "syslog-tls",
			egressMetric: egressMetric,
		},
	}

	return w
}
