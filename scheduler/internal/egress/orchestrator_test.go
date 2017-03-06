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
	It("writes syslog bindings to the writer", func() {
		reader := &SpyReader{
			drains: ingress.AppBindings{
				"app-id": ingress.Binding{
					Hostname: "org.space.app",
					Drains:   []string{"syslog://my-app-drain", "syslog://other-drain"},
				},
			},
		}
		writer := &SpyWriter{}

		o := egress.NewOrchestrator(reader, writer)
		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Eventually(writer.createdBindings, 2).Should(ContainElement(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://my-app-drain",
			},
		))
		Eventually(writer.createdBindings, 2).Should(ContainElement(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://other-drain",
			},
		))
	})

	It("does not write when the read fails", func() {
		reader := &SpyReader{
			err: errors.New("Nope!"),
		}
		writer := &SpyWriter{}

		o := egress.NewOrchestrator(reader, writer)
		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Consistently(writer.createdBindings).Should(HaveLen(0))
	})

	It("deletes bindings that are no longer present", func() {
		reader := &SpyReader{
			drains: ingress.AppBindings{
				"app-id": ingress.Binding{
					Hostname: "org.space.app",
					Drains:   []string{"syslog://my-app-drain"},
				},
			},
		}
		writer := &SpyWriter{
			listedBindings: [][]*v1.Binding{{
				&v1.Binding{
					AppId:    "app-id",
					Hostname: "org.space.app",
					Drain:    "syslog://my-app-drain",
				},
				&v1.Binding{
					AppId:    "app-id",
					Hostname: "org.space.app",
					Drain:    "syslog://other-drain",
				},
			}},
		}

		o := egress.NewOrchestrator(reader, writer)
		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Eventually(writer.deletedBindings, 2).Should(ContainElement(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://other-drain",
			},
		))
	})
})

type SpyWriter struct {
	createdBindings_ []*v1.Binding
	deletedBindings_ []*v1.Binding
	listedBindings   [][]*v1.Binding
	mu               sync.Mutex
}

func (s *SpyWriter) Create(binding *v1.Binding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createdBindings_ = append(s.createdBindings_, binding)
	return nil
}

func (s *SpyWriter) createdBindings() []*v1.Binding {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createdBindings_
}

func (s *SpyWriter) Delete(binding *v1.Binding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedBindings_ = append(s.deletedBindings_, binding)
	return nil
}

func (s *SpyWriter) deletedBindings() []*v1.Binding {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletedBindings_
}

func (s *SpyWriter) List() (bindings [][]*v1.Binding, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listedBindings, nil
}

type SpyReader struct {
	drains ingress.AppBindings
	err    error
}

func (s *SpyReader) FetchBindings() (appBindings ingress.AppBindings, err error) {
	return s.drains, s.err
}
