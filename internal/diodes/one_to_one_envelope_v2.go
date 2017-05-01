package diodes

import (
	"code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	gendiodes "github.com/cloudfoundry/diodes"
)

// OneToOne diode is optimized for a single writer and a single reader
type OneToOne struct {
	d *gendiodes.Poller
}

func NewOneToOne(size int, alerter gendiodes.Alerter) *OneToOne {
	return &OneToOne{
		d: gendiodes.NewPoller(gendiodes.NewOneToOne(size, alerter)),
	}
}

func (d *OneToOne) Set(data *loggregator_v2.Envelope) {
	d.d.Set(gendiodes.GenericDataType(data))
}

func (d *OneToOne) TryNext() (*loggregator_v2.Envelope, bool) {
	data, ok := d.d.TryNext()
	if !ok {
		return nil, ok
	}

	return (*loggregator_v2.Envelope)(data), true
}

func (d *OneToOne) Next() *loggregator_v2.Envelope {
	data := d.d.Next()
	return (*loggregator_v2.Envelope)(data)
}
