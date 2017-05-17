package egress_test

import (
	"errors"
	"sync"
	"time"

	v2 "code.cloudfoundry.org/scalable-syslog/internal/api/loggregator/v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter/testhelper"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Orchestrator", func() {
	var (
		healthEmitter  *SpyHealthEmitter
		adapterService *SpyAdapterService
		metricEmitter  *testhelper.SpyMetricClient
	)

	BeforeEach(func() {
		healthEmitter = &SpyHealthEmitter{}
		adapterService = &SpyAdapterService{}
		metricEmitter = testhelper.NewMetricClient()
	})

	It("writes syslog bindings to the writer", func() {
		reader := &SpyReader{
			drains: ingress.Bindings{
				v1.Binding{
					AppId:    "app-id",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			},
		}

		o := egress.NewOrchestrator(
			reader,
			adapterService,
			healthEmitter,
			metricEmitter,
		)
		go o.Run(time.Millisecond)

		Eventually(adapterService.CreateDeltaActual).Should(HaveLen(0))
		Eventually(adapterService.CreateDeltaExpected).Should(HaveLen(1))

		Eventually(adapterService.DeleteDeltaActual).Should(HaveLen(0))
		Eventually(adapterService.DeleteDeltaExpected).Should(HaveLen(1))
	})

	It("does not write when the read fails", func() {
		reader := &SpyReader{
			err: errors.New("Nope!"),
		}

		o := egress.NewOrchestrator(
			reader,
			adapterService,
			healthEmitter,
			metricEmitter,
		)
		go o.Run(time.Millisecond)

		Consistently(adapterService.CreateDeltaCalled).Should(BeFalse())
	})

	It("emits a metric for the number of drains", func() {
		reader := &SpyReader{
			drains: ingress.Bindings{
				v1.Binding{
					AppId:    "app-id",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			},
		}

		o := egress.NewOrchestrator(
			reader,
			adapterService,
			healthEmitter,
			metricEmitter,
		)
		go o.Run(time.Millisecond)

		f := func() v2.Envelope {
			var actualEnvelope *v2.Envelope
			metricEmitter.GaugeMetric.SendWith(func(e *v2.Envelope) error {
				actualEnvelope = e

				return nil
			})
			return *actualEnvelope
		}
		Eventually(f).Should(MatchFields(IgnoreExtras, Fields{
			"SourceId": Equal("spy-client"),
			"Tags": Equal(map[string]*v2.Value{
				"metric_version": {
					Data: &v2.Value_Text{
						Text: "2.0",
					},
				},
			}),
			"Message": Equal(&v2.Envelope_Gauge{
				Gauge: &v2.Gauge{
					Metrics: map[string]*v2.GaugeValue{
						"drains": {
							Unit:  "count",
							Value: 1,
						},
					},
				},
			}),
		}))
	})
})

type SpyReader struct {
	drains ingress.Bindings
	err    error
}

func (s *SpyReader) FetchBindings() (appBindings ingress.Bindings, invalid int, err error) {
	return s.drains, 0, s.err
}

type SpyHealthEmitter struct{}

func (s *SpyHealthEmitter) SetCounter(_ map[string]int) {}

type SpyAdapterService struct {
	mu                                     sync.Mutex
	createDeltaActual, createDeltaExpected ingress.Bindings
	deleteDeltaActual, deleteDeltaExpected ingress.Bindings
	createDeltaCalled                      bool
}

func (s *SpyAdapterService) List() ingress.Bindings {
	return nil
}

func (s *SpyAdapterService) CreateDelta(actual ingress.Bindings, expected ingress.Bindings) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.createDeltaCalled = true
	s.createDeltaActual = actual
	s.createDeltaExpected = expected
}

func (s *SpyAdapterService) DeleteDelta(actual ingress.Bindings, expected ingress.Bindings) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteDeltaActual = actual
	s.deleteDeltaExpected = expected
}

func (s *SpyAdapterService) CreateDeltaCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.createDeltaCalled
}

func (s *SpyAdapterService) CreateDeltaActual() ingress.Bindings {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.createDeltaActual
}

func (s *SpyAdapterService) CreateDeltaExpected() ingress.Bindings {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.createDeltaExpected
}

func (s *SpyAdapterService) DeleteDeltaActual() ingress.Bindings {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteDeltaActual
}

func (s *SpyAdapterService) DeleteDeltaExpected() ingress.Bindings {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteDeltaExpected
}
