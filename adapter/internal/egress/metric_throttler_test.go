package egress_test

import (
	"fmt"

	egress "github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MetricThrottler", func() {
	It("emits a metric after 1000 calls", func() {
		throttle := egress.NewMetricThrottler()

		for i := 1; i < 1000; i++ {
			throttle.Emit(func(_ int) {
				panic(fmt.Sprintf("We should never get here"))
			})
		}

		called := false
		throttle.Emit(func(count int) {
			Expect(count).To(Equal(1000))
			called = true
		})
		Expect(called).To(BeTrue())
	})
})
