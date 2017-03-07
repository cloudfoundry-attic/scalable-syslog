package retrystrategy

import (
	"math"
	"math/rand"
	"time"
)

// RetryStrategy returns a duration based on a counter.
type RetryStrategy func(counter int) time.Duration

// Exponential is a retry strategy that has a max of 1h9m54s retry duration.
func Exponential() RetryStrategy {
	return func(counter int) time.Duration {
		if counter == 0 {
			return time.Millisecond
		}
		if counter > 23 {
			counter = 23
		}
		tenthDuration := int(math.Pow(2, float64(counter-1)) * 100)
		duration := tenthDuration * 10
		randomOffset := rand.Intn(tenthDuration*2) - tenthDuration
		return (time.Duration(duration) * time.Microsecond) + (time.Duration(randomOffset) * time.Microsecond)
	}
}
