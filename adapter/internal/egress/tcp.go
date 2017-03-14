package egress

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress/retrystrategy"
	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/crewjam/rfc5424"
)

// DialFunc represents a method for creating a connection, either TCP or TLS.
type DialFunc func(addr string) (net.Conn, error)

// TCPWriter represents a syslog writer that connects over unencrypted TCP.
type TCPWriter struct {
	url           *url.URL
	appID         string
	hostname      string
	dialFunc      DialFunc
	retryStrategy retrystrategy.RetryStrategy
	ioTimeout     time.Duration

	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

// NewTCPWriter creates a new TCP syslog writer.
func NewTCPWriter(binding *v1.Binding, dialTimeout, ioTimeout time.Duration, skipCertVerify bool) (*TCPWriter, error) {
	drainURL, err := url.Parse(binding.Drain)
	// TODO: remove parsing/error from here
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{
		Timeout: dialTimeout,
	}
	df := func(addr string) (net.Conn, error) {
		return dialer.Dial("tcp", addr)
	}

	w := &TCPWriter{
		url:           drainURL,
		appID:         binding.AppId,
		hostname:      binding.Hostname,
		retryStrategy: retrystrategy.Exponential(),
		ioTimeout:     ioTimeout,
		dialFunc:      df,
	}
	go w.connect()

	return w, nil
}

func (w *TCPWriter) connect() {
	var retryCount int
	for {
		w.mu.Lock()
		closed := w.closed
		w.mu.Unlock()
		if closed {
			return
		}

		w.mu.Lock()
		conn, err := w.dialFunc(w.url.Host)
		if err != nil {
			w.mu.Unlock()
			duration := w.retryStrategy(retryCount)
			log.Printf("failed to connect to %s, retrying in %s: %s", w.url.Host, duration, err)
			time.Sleep(duration)
			retryCount++
			continue
		}

		log.Printf("created conn to syslog drain: %s", w.url.Host)

		w.conn = conn
		w.mu.Unlock()
		return
	}
}

// Close tears down any active connections to the drain and prevents reconnect.
func (w *TCPWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return w.connClose()
}

func (w *TCPWriter) connClose() error {
	if w.conn != nil {
		err := w.conn.Close()
		w.conn = nil

		return err
	}

	return nil
}

// Write writes an envelope to the syslog drain connection.
func (w *TCPWriter) Write(env *loggregator_v2.Envelope) error {
	if env.GetLog() == nil {
		return nil
	}

	msg := rfc5424.Message{
		Priority:  generatePriority(env.GetLog().Type),
		Timestamp: time.Unix(0, env.GetTimestamp()).UTC(),
		Hostname:  w.hostname,
		AppName:   w.appID,
		ProcessID: generateProcessID(
			env.Tags["source_type"].GetText(),
			env.Tags["source_instance"].GetText(),
		),
		Message: appendNewline(removeNulls(env.GetLog().Payload)),
	}

	w.mu.Lock()
	conn := w.conn
	w.mu.Unlock()

	if conn == nil {
		return errors.New("connection does not exist")
	}

	conn.SetWriteDeadline(time.Now().Add(w.ioTimeout))
	_, err := msg.WriteTo(conn)
	if err != nil {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.connClose()
		go w.connect()

		return err
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
