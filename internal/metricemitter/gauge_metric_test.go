package metricemitter_test

import (
	v2 "code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GaugeMetric", func() {
	It("calls the passed-in function with an envelope", func() {
		g := metricemitter.NewGaugeMetric("some-gauge", "some-unit", "source-id")

		g.Set(10)

		var actualEnvelope *v2.Envelope
		fn := func(e *v2.Envelope) error {
			actualEnvelope = e
			return nil
		}

		err := g.SendWith(fn)
		Expect(err).NotTo(HaveOccurred())

		Expect(actualEnvelope.GetGauge().Metrics).To(Equal(map[string]*v2.GaugeValue{
			"some-gauge": &v2.GaugeValue{
				Unit:  "some-unit",
				Value: 10,
			},
		}))
	})

	It("configures the sourceID", func() {
		g := metricemitter.NewGaugeMetric("some-gauge", "some-unit", "source-id")

		var actualEnvelope *v2.Envelope
		fn := func(e *v2.Envelope) error {
			actualEnvelope = e
			return nil
		}

		err := g.SendWith(fn)
		Expect(err).NotTo(HaveOccurred())

		Expect(actualEnvelope.SourceId).To(Equal("source-id"))
	})

	It("appends a timestamp", func() {
		g := metricemitter.NewGaugeMetric("some-gauge", "some-unit", "source-id")

		var actualEnvelope *v2.Envelope
		fn := func(e *v2.Envelope) error {
			actualEnvelope = e
			return nil
		}

		err := g.SendWith(fn)
		Expect(err).NotTo(HaveOccurred())

		Expect(actualEnvelope.Timestamp).To(BeNumerically(">", int64(0)))
	})

	It("adds tags", func() {
		g := metricemitter.NewGaugeMetric(
			"some-gauge",
			"some-unit",
			"source-id",
			metricemitter.WithTags(map[string]string{
				"some-tag": "some-value",
			}),
		)

		var actualEnvelope *v2.Envelope
		fn := func(e *v2.Envelope) error {
			actualEnvelope = e
			return nil
		}

		err := g.SendWith(fn)
		Expect(err).NotTo(HaveOccurred())

		Expect(actualEnvelope.Tags).To(Equal(map[string]*v2.Value{
			"some-tag": &v2.Value{
				Data: &v2.Value_Text{Text: "some-value"},
			},
		}))
	})
})
