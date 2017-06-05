package egress

import (
	"golang.org/x/net/context"

	"code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	"code.cloudfoundry.org/scalable-syslog/internal/diodes"
	gendiodes "github.com/cloudfoundry/diodes"
)

type WaitGroup interface {
	Add(delta int)
	Done()
}

type DiodeWriter struct {
	wc    WriteCloser
	diode *diodes.OneToOne
	wg    WaitGroup

	ctx context.Context
}

func NewDiodeWriter(
	ctx context.Context,
	wc WriteCloser,
	alerter gendiodes.Alerter,
	wg WaitGroup,
) *DiodeWriter {
	dw := &DiodeWriter{
		wc:    wc,
		diode: diodes.NewOneToOne(100, alerter, gendiodes.WithPollingContext(ctx)),
		wg:    wg,
		ctx:   ctx,
	}
	wg.Add(1)
	go dw.start()

	return dw
}

// Write writes an envelope into the diode. This can not fail.
func (d *DiodeWriter) Write(env *loggregator_v2.Envelope) error {
	d.diode.Set(env)

	return nil
}

func (d *DiodeWriter) start() {
	defer d.wc.Close()
	defer d.wg.Done()

	for {
		e := d.diode.Next()
		if e == nil {
			return
		}

		d.wc.Write(e)
	}
}
