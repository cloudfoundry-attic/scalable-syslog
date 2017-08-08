package egress

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// RetryWrapper wraps a WriterConstructer, allowing it to retry writes.
func RetryWrapper(
	wc WriterConstructor,
	r RetryDuration,
	maxRetries uint,
	logClient LogClient,
) WriterConstructor {
	return WriterConstructor(func(
		binding *URLBinding,
		dialTimeout time.Duration,
		ioTimeout time.Duration,
		skipCertVerify bool,
		egressMetric *pulseemitter.CounterMetric,
	) WriteCloser {
		writer := wc(
			binding,
			dialTimeout,
			ioTimeout,
			skipCertVerify,
			egressMetric,
		)

		return &RetryWriter{
			writer:        writer,
			retryDuration: r,
			maxRetries:    maxRetries,
			binding:       binding,
			logClient:     logClient,
		}
	})
}

// RetryDuration calculates a duration based on the number of write attempts.
type RetryDuration func(attempt uint) time.Duration

// RetryWriter wraps a WriteCloser and will retry writes if the first fails.
type RetryWriter struct {
	writer        WriteCloser
	retryDuration RetryDuration
	maxRetries    uint
	binding       *URLBinding
	logClient     LogClient
}

// Write will retry writes unitl maxRetries has been reached.
func (r *RetryWriter) Write(e *loggregator_v2.Envelope) error {
	err := r.writer.Write(e)

	if err != nil && !contextDone(r.binding.Context) {
		return r.retry(e)
	}

	return err
}

// Close delegates to the syslog writer.
func (r *RetryWriter) Close() error {
	return r.writer.Close()
}

func (r *RetryWriter) retry(e *loggregator_v2.Envelope) error {
	var err error

	option := loggregator.WithAppInfo(
		r.binding.AppID,
		"LGR",
		e.GetTags()["source_instance"].GetText(),
	)
	for i := uint(1); i < r.maxRetries; i++ {
		sleepDuration := r.retryDuration(i)
		msg := fmt.Sprintf("Syslog Drain: Error when writing. Backing off for %s.", sleepDuration)
		r.logClient.EmitLog(msg, option)
		log.Printf("failed to write to %s, retrying in %s: %s", r.binding.URL.Host, sleepDuration, err)

		time.Sleep(sleepDuration)

		err = r.writer.Write(e)
		if err == nil || contextDone(r.binding.Context) {
			break
		}
	}

	return err
}

// ExponentialDuration returns a duration that grows exponentially with each
// attempt. It is maxed out at about 35 minutes.
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
