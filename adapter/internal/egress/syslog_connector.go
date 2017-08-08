package egress

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"golang.org/x/net/context"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"github.com/cloudfoundry/diodes"
)

// Write is the interface for all diode writers.
type Writer interface {
	Write(*loggregator_v2.Envelope) error
}

// WriteCloser is the interface for all syslog writers.
type WriteCloser interface {
	Writer
	io.Closer
}

// LogClient is used to emit logs.
type LogClient interface {
	EmitLog(message string, opts ...loggregator.EmitLogOption)
}

// nullLogClient ensures that the LogClient is in fact optional.
type nullLogClient struct{}

// EmitLog drops all messages into /dev/null.
func (nullLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
}

// SyslogConnector creates the various egress syslog writers.
type SyslogConnector struct {
	skipCertVerify bool
	ioTimeout      time.Duration
	dialTimeout    time.Duration
	constructors   map[string]WriterConstructor
	droppedMetrics map[string]*pulseemitter.CounterMetric
	egressMetrics  map[string]*pulseemitter.CounterMetric
	logClient      LogClient
	wg             WaitGroup
}

// NewSyslogConnector configures and returns a new SyslogConnector.
func NewSyslogConnector(
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	wg WaitGroup,
	opts ...ConnectorOption,
) *SyslogConnector {
	sc := &SyslogConnector{
		ioTimeout:      ioTimeout,
		dialTimeout:    dialTimeout,
		skipCertVerify: skipCertVerify,
		wg:             wg,
		logClient:      nullLogClient{},
		constructors:   make(map[string]WriterConstructor),
		droppedMetrics: make(map[string]*pulseemitter.CounterMetric),
		egressMetrics:  make(map[string]*pulseemitter.CounterMetric),
	}
	for _, o := range opts {
		o(sc)
	}
	return sc
}

// WriterConstructor creates syslog connections to https, syslog, and
// syslog-tls drains
type WriterConstructor func(
	binding *URLBinding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric *pulseemitter.CounterMetric,
) WriteCloser

// ConnectorOption allows a syslog connector to be customized.
type ConnectorOption func(*SyslogConnector)

// WithConstructors allows users to configure the constructors which will
// create syslog network connections. The string key in the constructors map
// should name the protocol.
func WithConstructors(constructors map[string]WriterConstructor) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.constructors = constructors
	}
}

// WithDroppedMetrics allows users to configure the dropped metrics which will
// be emitted when a syslog writer drops messages
func WithDroppedMetrics(metrics map[string]*pulseemitter.CounterMetric) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.droppedMetrics = metrics
	}
}

// WithEgressMetrics allows users to configure the dropped metrics which will
// be emitted when a syslog writer drops messages
func WithEgressMetrics(metrics map[string]*pulseemitter.CounterMetric) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.egressMetrics = metrics
	}
}

func WithLogClient(logClient LogClient) ConnectorOption {
	return func(sc *SyslogConnector) {
		sc.logClient = logClient
	}
}

// URLBinding associates a particular application with a syslog URL. The
// application is identified by AppID and Hostname. The syslog URL is
// identified by URL.
type URLBinding struct {
	Context  context.Context
	AppID    string
	Hostname string
	URL      *url.URL
}

// Scheme is a convenience wrapper around the *url.URL Scheme field
func (u *URLBinding) Scheme() string {
	return u.URL.Scheme
}

func parseBinding(b *v1.Binding) (*URLBinding, error) {
	url, err := url.Parse(b.Drain)
	if err != nil {
		return nil, err
	}

	u := &URLBinding{
		AppID:    b.AppId,
		URL:      url,
		Hostname: b.Hostname,
	}

	return u, nil
}

// Connect returns an egress writer based on the scheme of the binding drain
// URL.
func (w *SyslogConnector) Connect(ctx context.Context, b *v1.Binding) (Writer, error) {
	urlBinding, err := parseBinding(b)
	if err != nil {
		w.emitErrorLog(b.AppId, "parse failure")
		return nil, err
	}
	urlBinding.Context = ctx

	droppedMetric := w.droppedMetrics[urlBinding.Scheme()]
	egressMetric := w.egressMetrics[urlBinding.Scheme()]
	constructor, ok := w.constructors[urlBinding.Scheme()]
	if !ok {
		w.emitErrorLog(b.AppId, "unsupported scheme")
		return nil, errors.New("unsupported scheme")
	}
	writer := constructor(urlBinding, w.dialTimeout, w.ioTimeout, w.skipCertVerify, egressMetric)

	dw := NewDiodeWriter(ctx, writer, diodes.AlertFunc(func(missed int) {
		if droppedMetric != nil {
			droppedMetric.Increment(uint64(missed))
		}

		log.Printf("Dropped %d %s logs", missed, urlBinding.Scheme())
	}), w.wg)

	return dw, nil
}

func (w *SyslogConnector) emitErrorLog(appID, message string) {
	option := loggregator.WithAppInfo(
		appID,
		"LGR",
		"", // source instance is unavailable
	)
	w.logClient.EmitLog(fmt.Sprintf("Invalid syslog drain URL: %s", message), option)
}
