package egress

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress/retrystrategy"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	"github.com/crewjam/rfc5424"
)

// TCPWriter represents a syslog writer that connects over unencrypted TCP.
type TCPWriter struct {
	url           url.URL
	appID         string
	hostname      string
	conn          net.Conn
	dialer        Dialer
	retryStrategy retrystrategy.RetryStrategy
}

// NewTCP creates a new TCP syslog writer.
func NewTCP(url url.URL, appID, hostname string, opts ...TCPOption) (*TCPWriter, error) {
	if url.Scheme != "syslog" {
		return nil, errors.New("invalid scheme for syslog tcp writer")
	}

	w := &TCPWriter{
		url:           url,
		appID:         appID,
		hostname:      hostname,
		dialer:        &net.Dialer{},
		retryStrategy: retrystrategy.Exponential(),
	}
	for _, o := range opts {
		o(w)
	}
	w.connect()

	return w, nil
}

func (w *TCPWriter) connect() {
	var retryCount int
	for {
		conn, err := w.dialer.Dial("tcp", w.url.Host)
		if err != nil {
			duration := w.retryStrategy(retryCount)
			log.Printf("failed to connect to %s, retrying in %s: %s", w.url.Host, duration, err)
			time.Sleep(duration)
			retryCount++
			continue
		}
		log.Printf("created conn to syslog drain: %s", w.url.Host)
		w.conn = conn
		return
	}
}

// TCPOption configures a TCPWriter.
type TCPOption func(*TCPWriter)

// WithTCPDialer overrides the default net.Dialer used for establishing conns.
func WithTCPDialer(d Dialer) TCPOption {
	return func(w *TCPWriter) {
		w.dialer = d
	}
}

// WithRetryStrategy overrides the default Exponential backoff strategy.
func WithRetryStrategy(r retrystrategy.RetryStrategy) TCPOption {
	return func(w *TCPWriter) {
		w.retryStrategy = r
	}
}

// Close tears down any active connections to the drain and prevents reconnect.
func (w *TCPWriter) Close() error {
	if err := w.conn.Close(); err != nil {
		return err
	}
	w.conn = nil

	return nil
}

// Write writes an envelope to the syslog drain connection.
func (w *TCPWriter) Write(env *loggregator_v2.Envelope) error {
	if w.conn == nil {
		return errors.New("connection does not exist")
	}

	if env.GetLog() == nil {
		return nil
	}

	msg := rfc5424.Message{
		Priority:  generatePriority(env.GetLog().Type),
		Timestamp: time.Unix(0, env.GetTimestamp()),
		Hostname:  w.hostname,
		AppName:   w.appID,
		ProcessID: generateProcessID(
			env.Tags["source_type"].GetText(),
			env.Tags["source_instance"].GetText(),
		),
		Message: appendNewline(removeNulls(env.GetLog().Payload)),
	}

	_, err := msg.WriteTo(w.conn)
	if err != nil {
		w.Close()
		w.connect()
	}

	return nil
}

func removeNulls(msg []byte) []byte {
	return bytes.Replace(msg, []byte{0}, nil, -1)
}

func appendNewline(msg []byte) []byte {
	if !bytes.HasSuffix(msg, []byte("\n")) {
		msg = append(msg, byte('\n'))
	}
	return msg
}

func generatePriority(logType loggregator_v2.Log_Type) rfc5424.Priority {
	switch logType {
	case loggregator_v2.Log_OUT:
		return rfc5424.Info + rfc5424.User
	case loggregator_v2.Log_ERR:
		return rfc5424.Error + rfc5424.User
	default:
		return rfc5424.Priority(-1)
	}
}

func generateProcessID(sourceType, sourceInstance string) string {
	sourceType = strings.ToUpper(sourceType)
	if strings.HasPrefix(sourceType, "APP") {
		return fmt.Sprintf("[%s/%s]", sourceType, sourceInstance)
	}
	return fmt.Sprintf("[%s]", sourceType)
}
