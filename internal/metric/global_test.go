package metric_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/metric"
)

var _ = Describe("default emitter", func() {
	var (
		receiver <-chan *loggregator_v2.Envelope
		addr     string
	)

	Context("with a default emitter setup", func() {
		BeforeEach(func() {
			var spyIngressServer *SpyIngressServer
			addr, spyIngressServer = startIngressServer()

			metric.Setup(
				metric.WithAddr(addr),
				metric.WithBatchInterval(time.Millisecond),
			)

			metric.IncCounter("seed-data")

			rx := fetchReceiver(spyIngressServer)
			receiver = rxToCh(rx)
		})

		It("can increment a counter", func() {
			metric.IncCounter("foo", metric.WithIncrement(42))
			var e *loggregator_v2.Envelope
			f := func() bool {
				Eventually(receiver).Should(Receive(&e))
				return e.GetCounter().Name == "foo"
			}
			Eventually(f).Should(BeTrue())
			Expect(e.GetCounter().GetDelta()).To(Equal(uint64(42)))
		})
	})
})
