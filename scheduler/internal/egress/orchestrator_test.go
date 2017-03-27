package egress_test

import (
	"errors"
	"sync"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		healthEmitter  *SpyHealthEmitter
		adapterService *SpyAdapterService
	)

	BeforeEach(func() {
		healthEmitter = &SpyHealthEmitter{}
		adapterService = &SpyAdapterService{}
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

		o := egress.NewOrchestrator(reader, adapterService, healthEmitter)
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

		o := egress.NewOrchestrator(reader, adapterService, healthEmitter)
		go o.Run(1 * time.Millisecond)

		Consistently(adapterService.CreateDeltaCalled).Should(BeFalse())
	})
})

type SpyReader struct {
	drains ingress.Bindings
	err    error
}

func (s *SpyReader) FetchBindings() (appBindings ingress.Bindings, err error) {
	return s.drains, s.err
}

type SpyHealthEmitter struct{}

func (s *SpyHealthEmitter) SetCounter(_ map[string]int) {}

type SpyAdapterService struct {
	mu                                     sync.Mutex
	createDeltaActual, createDeltaExpected ingress.Bindings
	deleteDeltaActual, deleteDeltaExpected ingress.Bindings
	createDeltaCalled                      bool
}

func (s *SpyAdapterService) List() (ingress.Bindings, error) {
	return nil, nil
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
