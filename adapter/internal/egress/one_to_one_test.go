package egress_test

import (
	"sync"
	"sync/atomic"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("OneToOne", func() {
	var (
		d   *egress.OneToOne
		env *loggregator_v2.Envelope

		spyAlerter *SpyAlerter
	)

	BeforeEach(func() {
		spyAlerter = &SpyAlerter{}

		d = egress.NewOneToOne(5, spyAlerter)
	})

	Describe("Next()", func() {
		BeforeEach(func() {
			env = &loggregator_v2.Envelope{
				SourceId: "test-source-id",
			}
			d.Set(env)
		})

		It("returns the next env", func() {
			Expect(d.Next()).To(Equal(env))
		})

		Context("multiple envs", func() {
			var secondEnv *loggregator_v2.Envelope

			BeforeEach(func() {
				secondEnv = &loggregator_v2.Envelope{
					SourceId: "second-test-source-id",
				}
				d.Set(secondEnv)
			})

			It("returns envs in order", func() {
				Expect(d.Next()).To(Equal(env))
				Expect(d.Next()).To(Equal(secondEnv))
			})

			Context("reads exceed writes", func() {
				var (
					rxCh chan *loggregator_v2.Envelope
					wg   sync.WaitGroup
				)

				var waitForNext = func() {
					defer wg.Done()
					rxCh <- d.Next()
				}

				BeforeEach(func() {
					rxCh = make(chan *loggregator_v2.Envelope, 100)
					for i := 0; i < 2; i++ {
						d.Next()
					}
					wg.Add(1)
					go waitForNext()
				})

				AfterEach(func() {
					wg.Wait()
				})

				It("blocks until env is available", func() {
					Consistently(rxCh).Should(HaveLen(0))
					d.Set(env)
					Eventually(rxCh).Should(HaveLen(1))
				})
			})
		})
	})

	Describe("TryNext()", func() {
		BeforeEach(func() {
			env := &loggregator_v2.Envelope{
				SourceId: "test-source-id",
			}
			d.Set(env)
		})

		It("returns true", func() {
			_, ok := d.TryNext()

			Expect(ok).To(BeTrue())
		})

		Context("reads exceed writes", func() {
			BeforeEach(func() {
				d.TryNext()
				d.TryNext()
			})

			It("returns false", func() {
				_, ok := d.TryNext()

				Expect(ok).To(BeFalse())
			})
		})
	})

	Describe("Set()", func() {
		BeforeEach(func() {
			env := &loggregator_v2.Envelope{
				SourceId: "test-source-id",
			}

			for i := 0; i < 5; i++ {
				d.Set(env)
			}
		})

		It("alerts for each dropped point", func() {
			d.Set(env)
			d.Next()
			Expect(spyAlerter.missed()).To(Equal(int64(5)))
		})

		It("it updates the read index", func() {
			d.Set(env)
			d.Next()
			d.Next()
			Expect(spyAlerter.missed()).To(Equal(int64(5)))

			for i := 0; i < 6; i++ {
				d.Set(env)
			}

			d.Next()
			Expect(spyAlerter.missed()).To(Equal(int64(5)))
		})

		Context("read catches up with write", func() {
			It("does not alert", func() {
				d.Next()
				d.Next()
				Expect(spyAlerter.missed()).To(Equal(int64(0)))
			})
		})

		Context("writer laps reader", func() {
			BeforeEach(func() {
				for i := 0; i < 6; i++ {
					d.Set(env)
				}
				d.Next()
			})

			It("sends an alert for each set", func() {
				Expect(spyAlerter.missed()).To(Equal(int64(10)))
			})
		})
	})
})

type SpyAlerter struct {
	missed_ int64
}

func (s *SpyAlerter) Alert(missed int) {
	atomic.AddInt64(&s.missed_, int64(missed))
}

func (s *SpyAlerter) missed() int64 {
	return atomic.LoadInt64(&s.missed_)
}
