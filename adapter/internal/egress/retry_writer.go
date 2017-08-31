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
	maxRetries int,
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
type RetryDuration func(attempt int) time.Duration

// RetryWriter wraps a WriteCloser and will retry writes if the first fails.
type RetryWriter struct {
	writer        WriteCloser
	retryDuration RetryDuration
	maxRetries    int
	binding       *URLBinding
	logClient     LogClient
}

// Write will retry writes unitl maxRetries has been reached.
func (r *RetryWriter) Write(e *loggregator_v2.Envelope) error {
	logMsgOption := loggregator.WithAppInfo(
		r.binding.AppID,
		"LGR",
		e.GetTags()["source_instance"],
	)
	logMsgTemplate := "Syslog Drain: Error when writing. Backing off for %s."
	logTemplate := "failed to write to %s, retrying in %s, err: %s"

	var err error

	for i := 0; i < r.maxRetries; i++ {
		err = r.writer.Write(e)
		if err == nil {
			return nil
		}

		if contextDone(r.binding.Context) {
			return err
		}

		sleepDuration := r.retryDuration(i)
		log.Printf(logTemplate, r.binding.URL.Host, sleepDuration, err)
		msg := fmt.Sprintf(logMsgTemplate, sleepDuration)
		r.logClient.EmitLog(msg, logMsgOption)

		time.Sleep(sleepDuration)
	}

	return err
}

// Close delegates to the syslog writer.
func (r *RetryWriter) Close() error {
	return r.writer.Close()
}

// ExponentialDuration returns a duration that grows exponentially with each
// attempt. It is maxed out at about 35 minutes.
func ExponentialDuration(attempt int) time.Duration {
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
