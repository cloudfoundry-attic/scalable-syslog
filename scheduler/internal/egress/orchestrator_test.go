package egress_test

import (
	"errors"
	"sync"
	"time"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metric"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		healthEmitter  *SpyHealthEmitter
		adapterService *SpyAdapterService
		metricEmitter  *spyMetricEmitter
	)

	BeforeEach(func() {
		healthEmitter = &SpyHealthEmitter{}
		adapterService = &SpyAdapterService{}
		metricEmitter = newSpyMetricEmitter()
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
		go o.Run(1 * time.Millisecond)

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
		go o.Run(1 * time.Millisecond)

		Consistently(adapterService.CreateDeltaCalled).Should(BeFalse())
	})

	It("does not write when list fails", func() {
		reader := &SpyReader{}
		adapterService := &SpyAdapterService{
			Err: errors.New("an error"),
		}

		o := egress.NewOrchestrator(
			reader,
			adapterService,
			healthEmitter,
			metricEmitter,
		)
		go o.Run(1 * time.Millisecond)

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
		go o.Run(1 * time.Millisecond)

		Eventually(metricEmitter.name).Should(Receive(Equal("drains")))
		Expect(metricEmitter.opts).To(Receive(HaveLen(2)))
	})
})

type spyMetricEmitter struct {
	name chan string
	opts chan []metric.IncrementOpt
}

func newSpyMetricEmitter() *spyMetricEmitter {
	return &spyMetricEmitter{
		name: make(chan string, 10),
		opts: make(chan []metric.IncrementOpt, 10),
	}
}

func (e *spyMetricEmitter) IncCounter(name string, options ...metric.IncrementOpt) {
	e.name <- name
	e.opts <- options
}

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
	Err                                    error
	mu                                     sync.Mutex
	createDeltaActual, createDeltaExpected ingress.Bindings
	deleteDeltaActual, deleteDeltaExpected ingress.Bindings
	createDeltaCalled                      bool
}

func (s *SpyAdapterService) List() (ingress.Bindings, error) {
	return nil, s.Err
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
