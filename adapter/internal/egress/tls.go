package egress

import (
	"crypto/tls"
	"net"
	"net/url"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

// TLSWriter represents a syslog writer that connects over unencrypted TCP.
type TLSWriter struct {
	TCPWriter
}

func NewTLSWriter(binding *v1.Binding, dialTimeout, ioTimeout time.Duration, skipCertVerify bool) (WriteCloser, error) {
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

	w := &TLSWriter{}
	w.url = drainURL
	w.appID = binding.AppId
	w.hostname = binding.Hostname
	w.retryStrategy = Exponential()
	w.ioTimeout = ioTimeout
	w.dialFunc = df
	go w.connect()

	return w, nil
}
