package egress

import (
	"log"
	"math"
	"math/rand"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// RetryDuration calculates a duration based on the number of write attempts
type RetryDuration func(attempt uint) time.Duration

// RetryWriter wraps a WriteCloser and will retry writes if the first fails
type RetryWriter struct {
	syslog        WriteCloser
	retryDuration RetryDuration
	maxRetries    uint
	binding       *URLBinding
}

// NewRetryWriter creates a new SyslogConstructor which wraps another
// SyslogConstructor
func NewRetryWriter(
	sc SyslogConstructor,
	r RetryDuration,
	maxRetries uint,
) SyslogConstructor {

	return SyslogConstructor(func(
		binding *URLBinding,
		dialTimeout time.Duration,
		ioTimeout time.Duration,
		skipCertVerify bool,
		egressMetric *pulseemitter.CounterMetric,
	) WriteCloser {
		syslog := sc(binding, dialTimeout, ioTimeout, skipCertVerify, egressMetric)

		return &RetryWriter{
			syslog:        syslog,
			retryDuration: r,
			maxRetries:    maxRetries,
			binding:       binding,
		}
	})
}

// Write will retry writes unitl maxRetries has been reached
func (r *RetryWriter) Write(e *loggregator_v2.Envelope) error {
	err := r.syslog.Write(e)

	if err != nil && !contextDone(r.binding.Context) {
		return r.retry(e)
	}

	return err
}

// Close delegates to the syslog writer.
func (r *RetryWriter) Close() error {
	return r.syslog.Close()
}

func (r *RetryWriter) retry(e *loggregator_v2.Envelope) error {
	var err error

	for i := uint(1); i < r.maxRetries; i++ {
		sleepDuration := r.retryDuration(i)
		log.Printf("failed to write to %s, retrying in %s: %s", r.binding.URL.Host, sleepDuration, err)
		time.Sleep(sleepDuration)

		err = r.syslog.Write(e)
		if err == nil || contextDone(r.binding.Context) {
			break
		}
	}

	return err
}

func ExponentialDuration(attempt uint) time.Duration {
	if attempt == 0 {
		return time.Millisecond
	}
	if attempt > 22 {
		// setting attempts to 22 will result in one hour total
		// retrying writes
		attempt = 22
	}

	tenthDuration := int(math.Pow(2, float64(attempt-1)) * 100)
	duration := tenthDuration * 10

	randomOffset := rand.Intn(tenthDuration*2) - tenthDuration
	offset := time.Duration(randomOffset) * time.Microsecond

	return (time.Duration(duration) * time.Microsecond) + offset
}
