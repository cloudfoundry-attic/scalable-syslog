package egress

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/crewjam/rfc5424"
)

// DialFunc represents a method for creating a connection, either TCP or TLS.
type DialFunc func(addr string) (net.Conn, error)

// TCPWriter represents a syslog writer that connects over unencrypted TCP.
// This writer is not meant to be used from multiple goroutines. The same
// goroutine that calls `.Write()` should be the one that calls `.Close()`.
type TCPWriter struct {
	url       *url.URL
	appID     string
	hostname  string
	dialFunc  DialFunc
	ioTimeout time.Duration
	scheme    string
	conn      net.Conn

	egressMetric *pulseemitter.CounterMetric
}

// NewTCPWriter creates a new TCP syslog writer.
func NewTCPWriter(
	binding *URLBinding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric *pulseemitter.CounterMetric,
) WriteCloser {
	dialer := &net.Dialer{
		Timeout: dialTimeout,
	}
	df := func(addr string) (net.Conn, error) {
		return dialer.Dial("tcp", addr)
	}

	w := &TCPWriter{
		url:          binding.URL,
		appID:        binding.AppID,
		hostname:     binding.Hostname,
		ioTimeout:    ioTimeout,
		dialFunc:     df,
		scheme:       "syslog",
		egressMetric: egressMetric,
	}

	return w
}

func (w *TCPWriter) connection() (net.Conn, error) {
	if w.conn == nil {
		return w.connect()
	}
	return w.conn, nil
}

func (w *TCPWriter) connect() (net.Conn, error) {
	conn, err := w.dialFunc(w.url.Host)
	if err != nil {
		return nil, err
	}
	w.conn = conn

	log.Printf("created conn to syslog drain: %s", w.url.Host)

	return conn, nil
}

// Close tears down any active connections to the drain and prevents reconnect.
func (w *TCPWriter) Close() error {
	if w.conn != nil {
		err := w.conn.Close()
		w.conn = nil

		return err
	}

	return nil
}

func generateRFC5424Message(
	env *loggregator_v2.Envelope,
	hostname string,
	appID string,
) rfc5424.Message {
	return rfc5424.Message{
		Priority:  generatePriority(env.GetLog().Type),
		Timestamp: time.Unix(0, env.GetTimestamp()).UTC(),
		Hostname:  hostname,
		AppName:   appID,
		ProcessID: generateProcessID(
			env.Tags["source_type"].GetText(),
			env.Tags["source_instance"].GetText(),
		),
		Message: appendNewline(removeNulls(env.GetLog().Payload)),
	}
}

// Write writes an envelope to the syslog drain connection.
func (w *TCPWriter) Write(env *loggregator_v2.Envelope) error {
	if env.GetLog() == nil {
		return nil
	}

	msg := generateRFC5424Message(env, w.hostname, w.appID)
	conn, err := w.connection()
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(w.ioTimeout))
	_, err = msg.WriteTo(conn)
	if err != nil {
		_ = w.Close()

		return err
	}

	w.egressMetric.Increment(1)

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
	if sourceInstance != "" {
		return fmt.Sprintf("[%s/%s]", sourceType, sourceInstance)
	}

	return fmt.Sprintf("[%s]", sourceType)
}
