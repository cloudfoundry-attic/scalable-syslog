package egress

import (
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
)

type DiodeWriter struct {
	wc    WriteCloser
	diode *OneToOne
	done_ chan struct{}
}

func NewDiodeWriter(wc WriteCloser, alerter Alerter) *DiodeWriter {
	dw := &DiodeWriter{
		wc:    wc,
		diode: NewOneToOne(100, alerter),
		done_: make(chan struct{}),
	}
	go dw.start()
	return dw
}

func (d *DiodeWriter) start() {
	for {
		if d.done() {
			return
		}
		d.attemptMessageTransfer()
	}
}

func (d *DiodeWriter) done() bool {
	select {
	case <-d.done_:
		return true
	default:
		return false
	}
}

func (d *DiodeWriter) attemptMessageTransfer() {
	env, ok := d.diode.TryNext()
	if !ok {
		time.Sleep(10 * time.Millisecond)
		return
	}

	// TODO: do something with error?
	d.wc.Write(env)
}

// Write writes an envelope into the diode. This can not fail.
func (d *DiodeWriter) Write(env *loggregator_v2.Envelope) error {
	d.diode.Set(env)
	return nil
}

// Close tearsdown the goroutine servicing the diode and also closes the
// underlying writer, returning it's error.
func (d *DiodeWriter) Close() error {
	close(d.done_)
	return d.wc.Close()
}
