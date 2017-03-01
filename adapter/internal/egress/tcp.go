package egress

import (
	"errors"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	"github.com/flynn/flynn/pkg/syslog/rfc5424"
	"github.com/golang/protobuf/proto"
)

// TCPWriter represents a syslog writer that connects over unencrypted TCP.
type TCPWriter struct {
	url      url.URL
	appID    string
	hostname string
	mu       sync.Mutex
	conn     net.Conn
	dialer   Dialer
}

// NewTCP creates a new TCP syslog writer.
func NewTCP(url url.URL, opts ...TCPOption) (*TCPWriter, error) {
	if url.Scheme != "syslog" {
		return nil, errors.New("invalid scheme for syslog tcp writer")
	}

	w := &TCPWriter{
		url:    url,
		dialer: &net.Dialer{},
	}
	for _, o := range opts {
		o(w)
	}
	go w.connect()
	return w, nil
}

func (w *TCPWriter) connect() {
	for {
		conn, err := w.dialer.Dial("tcp", w.url.Host)
		if err != nil {
			// log?
			time.Sleep(time.Second) // exponential backoff?
			continue
		}
		w.mu.Lock()
		defer w.mu.Unlock()
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

// Close tears down any active connections to the drain and prevents reconnect.
func (*TCPWriter) Close() error {
	return nil
}

// Write writes an envelope to the syslog drain connection.
func (w *TCPWriter) Write(env *loggregator_v2.Envelope) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		return errors.New("unable to write to syslog drain")
	}
	bytes, err := proto.Marshal(env)
	// TODO: handle err
	_ = err
	header := &rfc5424.Header{
		//Priority, see below
		Timestamp: time.Unix(0, env.GetTimestamp()),
		Hostname:  []byte("some-hostname"), // passed into constructor
		AppName:   []byte("some-appname"),  // passed into constructor
		//ProcID: construct [APP/STUFF]
	}
	msg := rfc5424.NewMessage(header, bytes)
	_, err = w.conn.Write(msg.Bytes())

	return err
}

//switch msg.GetMessageType() {¬
//case events.LogMessage_OUT:¬
// return 14¬
//case events.LogMessage_ERR:¬
//return 11¬
//default:¬
//return -1¬
//}¬
