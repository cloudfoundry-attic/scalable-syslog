package egress

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
)

type Alerter interface {
	Alert(missed int)
}

type bucket struct {
	env *loggregator_v2.Envelope
	seq uint64
}

// OneToOne diode is optimized for a single writer and a single reader
type OneToOne struct {
	buffer     []unsafe.Pointer
	writeIndex uint64
	readIndex  uint64
	alerter    Alerter
}

var NewOneToOne = func(size int, alerter Alerter) *OneToOne {
	d := &OneToOne{
		buffer:  make([]unsafe.Pointer, size),
		alerter: alerter,
	}
	d.writeIndex = ^d.writeIndex
	return d
}

func (d *OneToOne) Set(env *loggregator_v2.Envelope) {
	writeIndex := atomic.AddUint64(&d.writeIndex, 1)
	idx := writeIndex % uint64(len(d.buffer))
	newBucket := &bucket{
		env: env,
		seq: writeIndex,
	}

	atomic.StorePointer(&d.buffer[idx], unsafe.Pointer(newBucket))
}

func (d *OneToOne) TryNext() (*loggregator_v2.Envelope, bool) {
	readIndex := atomic.LoadUint64(&d.readIndex)
	idx := readIndex % uint64(len(d.buffer))

	value, ok := d.tryNext(idx)
	if ok {
		atomic.AddUint64(&d.readIndex, 1)
	}
	return value, ok
}

func (d *OneToOne) Next() *loggregator_v2.Envelope {
	readIndex := atomic.LoadUint64(&d.readIndex)
	idx := readIndex % uint64(len(d.buffer))

	result := d.pollBuffer(idx)
	atomic.AddUint64(&d.readIndex, 1)

	return result
}

func (d *OneToOne) tryNext(idx uint64) (*loggregator_v2.Envelope, bool) {
	result := (*bucket)(atomic.SwapPointer(&d.buffer[idx], nil))

	if result == nil {
		return nil, false
	}

	if result.seq > d.readIndex {
		d.alerter.Alert(int(result.seq - d.readIndex))
		atomic.StoreUint64(&d.readIndex, result.seq)
	}

	return result.env, true
}

func (d *OneToOne) pollBuffer(idx uint64) *loggregator_v2.Envelope {
	for {
		result, ok := d.tryNext(idx)

		if !ok {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		return result
	}
}

type AlertFunc func(missed int)

func (f AlertFunc) Alert(missed int) {
	f(missed)
}
