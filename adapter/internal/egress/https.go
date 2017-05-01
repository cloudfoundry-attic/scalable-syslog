package egress

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"

	"code.cloudfoundry.org/scalable-syslog/internal/api"
	"code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metric"
	"github.com/crewjam/rfc5424"
)

type HTTPSWriter struct {
	binding        *v1.Binding
	client         *http.Client
	emitter        MetricEmitter
	metricThrottle *metricThrottler
}

func NewHTTPSWriter(
	binding *v1.Binding,
	dialTimeout,
	ioTimeout time.Duration,
	skipCertVerify bool,
	emitter MetricEmitter,
) (WriteCloser, error) {
	u, _ := url.Parse(binding.Drain)

	if u.Scheme != "https" {
		return nil, fmt.Errorf("invalid scheme for syslog HTTPWriter: %s", u.Scheme)
	}

	client := httpClient(dialTimeout, ioTimeout, skipCertVerify)

	return &HTTPSWriter{
		binding:        binding,
		client:         client,
		emitter:        emitter,
		metricThrottle: NewMetricThrottler(),
	}, nil
}

func (w *HTTPSWriter) Write(env *loggregator_v2.Envelope) error {
	msg := rfc5424.Message{
		Priority:  generatePriority(env.GetLog().Type),
		Timestamp: time.Unix(0, env.GetTimestamp()).UTC(),
		Hostname:  w.binding.Hostname,
		AppName:   w.binding.AppId,
		ProcessID: generateProcessID(
			env.Tags["source_type"].GetText(),
			env.Tags["source_instance"].GetText(),
		),
		Message: appendNewline(removeNulls(env.GetLog().Payload)),
	}
	b, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	resp, err := w.client.Post(w.binding.Drain, "text/plain", bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("Syslog Writer: Post responded with %d status code", resp.StatusCode)
	}

	io.Copy(ioutil.Discard, resp.Body)

	w.metricThrottle.Emit(func(metricCount int) {
		w.emitter.IncCounter(
			"egress",
			metric.WithIncrement(uint64(metricCount)),
			metric.WithVersion(2, 0),
			metric.WithTag("drain-protocol", "https"),
		)
		log.Printf("Egressed %d https messages", metricCount)
	})

	return nil
}

func (*HTTPSWriter) Close() error {
	return nil
}

func httpClient(dialTimeout, ioTimeout time.Duration, skipCertVerify bool) *http.Client {
	tlsConfig := api.NewTLSConfig()
	tlsConfig.InsecureSkipVerify = skipCertVerify

	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
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
