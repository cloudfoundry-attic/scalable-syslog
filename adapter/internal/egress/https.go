package egress

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
)

type HTTPSWriter struct {
	hostname     string
	appID        string
	url          *url.URL
	client       *http.Client
	egressMetric pulseemitter.CounterMetric
}

func NewHTTPSWriter(
	binding *URLBinding,
	keepalive time.Duration,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric pulseemitter.CounterMetric,
) WriteCloser {

	client := httpClient(keepalive, dialTimeout, ioTimeout, skipCertVerify)

	return &HTTPSWriter{
		url:          binding.URL,
		appID:        binding.AppID,
		hostname:     binding.Hostname,
		client:       client,
		egressMetric: egressMetric,
	}
}

func (w *HTTPSWriter) Write(env *loggregator_v2.Envelope) error {
	msgs := generateRFC5424Messages(env, w.hostname, w.appID)
	for _, msg := range msgs {
		b, err := msg.MarshalBinary()
		if err != nil {
			return err
		}

		resp, err := w.client.Post(w.url.String(), "text/plain", bytes.NewBuffer(b))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("Syslog Writer: Post responded with %d status code", resp.StatusCode)
		}

		io.Copy(ioutil.Discard, resp.Body)

		w.egressMetric.Increment(1)
	}

	return nil
}

func (*HTTPSWriter) Close() error {
	return nil
}

func httpClient(keepalive, dialTimeout, ioTimeout time.Duration, skipCertVerify bool) *http.Client {
	tlsConfig := api.NewTLSConfig()
	tlsConfig.InsecureSkipVerify = skipCertVerify

	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: keepalive,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsConfig,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}
}
