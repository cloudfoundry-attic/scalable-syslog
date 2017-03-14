package egress

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/api"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/crewjam/rfc5424"
)

type HTTPSWriter struct {
	binding *v1.Binding
	client  *http.Client
}

func NewHTTPSWriter(binding *v1.Binding, dialTimeout, ioTimeout time.Duration, skipCertVerify bool) (*HTTPSWriter, error) {
	u, _ := url.Parse(binding.Drain)

	if u.Scheme != "https" {
		return nil, fmt.Errorf("invalid scheme for syslog HTTPWriter: %s", u.Scheme)
	}

	client := httpClient(dialTimeout, ioTimeout, skipCertVerify)

	return &HTTPSWriter{
		binding: binding,
		client:  client,
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
	reader := bytes.NewBuffer(b)

	response, err := w.client.Post(w.binding.Drain, "text/plain", reader)
	if err != nil {
		return err
	}

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("Syslog Writer: Post responded with %d status code", response.StatusCode)
	}

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
