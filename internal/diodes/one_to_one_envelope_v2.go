package diodes

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	gendiodes "code.cloudfoundry.org/go-diodes"
)

// OneToOne diode is optimized for a single writer and a single reader
type OneToOne struct {
	d *gendiodes.Poller
}

func NewOneToOne(size int, alerter gendiodes.Alerter, opts ...gendiodes.PollerConfigOption) *OneToOne {
	return &OneToOne{
		d: gendiodes.NewPoller(gendiodes.NewOneToOne(size, alerter), opts...),
	}
}

func (d *OneToOne) Set(data *loggregator_v2.Envelope) {
	d.d.Set(gendiodes.GenericDataType(data))
}

func (d *OneToOne) Next() *loggregator_v2.Envelope {
	data := d.d.Next()
	return (*loggregator_v2.Envelope)(data)
}
