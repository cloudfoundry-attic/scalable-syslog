package egress_test

import (
	"errors"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/egress"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var healthEmitter *SpyHealthEmitter

	BeforeEach(func() {
		healthEmitter = &SpyHealthEmitter{}
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
		client := &SpyClient{
			listBindingsResponse_: &v1.ListBindingsResponse{},
		}

		o := egress.NewOrchestrator(reader, egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter), healthEmitter)
		go o.Run(1 * time.Millisecond)

		Eventually(client.createBindingRequest, 2).Should(Equal(
			&v1.CreateBindingRequest{
				&v1.Binding{
					AppId:    "app-id",
					Hostname: "org.space.app",
					Drain:    "syslog://my-app-drain",
				},
			},
		))
	})

	It("does not write when the read fails", func() {
		reader := &SpyReader{
			err: errors.New("Nope!"),
		}
		client := &SpyClient{}

		o := egress.NewOrchestrator(reader, egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter), healthEmitter)
		go o.Run(1 * time.Millisecond)

		Consistently(client.createBindingRequest).Should(BeNil())
	})

	It("deletes bindings that are no longer present", func() {
		reader := &SpyReader{
			drains: ingress.Bindings{
				v1.Binding{
					AppId:    "app-id",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			},
		}
		client := &SpyClient{
			listBindingsResponse_: &v1.ListBindingsResponse{
				Bindings: []*v1.Binding{
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
				},
			},
		}

		o := egress.NewOrchestrator(reader, egress.NewAdapterService(egress.AdapterPool{client}, healthEmitter), healthEmitter)
		go o.Run(1 * time.Millisecond)

		Eventually(client.deleteBindingRequest, 2).Should(Equal(
			&v1.DeleteBindingRequest{
				&v1.Binding{
					AppId:    "app-id",
					Hostname: "org.space.app",
					Drain:    "syslog://other-drain",
				},
			},
		))
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
