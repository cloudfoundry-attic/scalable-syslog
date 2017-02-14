package app_test

import (
	"errors"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	It("writes syslog bindings to the writer", func() {
		mockReader := newMockBindingReader()
		mockReader.FetchBindingsOutput.AppBindings <- app.AppBindings{
			"app-id": app.Binding{
				Hostname: "org.space.app",
				Drains:   []string{"syslog://my-app-drain", "syslog://other-drain"},
			},
		}
		close(mockReader.FetchBindingsOutput.Err)
		close(mockReader.FetchBindingsOutput.AppBindings)
		mockPool := newMockAdapterPool()
		close(mockPool.ListOutput.Bindings)
		close(mockPool.ListOutput.Err)
		close(mockPool.CreateOutput.Err)
		close(mockPool.DeleteOutput.Err)

		o := app.NewOrchestrator(mockReader, mockPool)
		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Eventually(mockPool.CreateInput.Binding, 2).Should(Receive(Equal(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://my-app-drain",
			},
		)))
		Eventually(mockPool.CreateInput.Binding, 2).Should(Receive(Equal(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://other-drain",
			},
		)))
	})

	It("does not write when the read fails", func() {
		mockReader := newMockBindingReader()
		mockReader.FetchBindingsOutput.Err <- errors.New("Nope!")
		close(mockReader.FetchBindingsOutput.Err)
		close(mockReader.FetchBindingsOutput.AppBindings)

		mockPool := newMockAdapterPool()
		close(mockPool.ListOutput.Bindings)
		close(mockPool.ListOutput.Err)
		close(mockPool.CreateOutput.Err)
		close(mockPool.DeleteOutput.Err)

		o := app.NewOrchestrator(mockReader, mockPool)
		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Consistently(mockPool.CreateCalled).ShouldNot(Receive())
	})

	It("deletes bindings that are no longer present", func() {
		mockReader := newMockBindingReader()
		mockReader.FetchBindingsOutput.AppBindings <- app.AppBindings{
			"app-id": app.Binding{
				Hostname: "org.space.app",
				Drains:   []string{"syslog://my-app-drain"},
			},
		}
		close(mockReader.FetchBindingsOutput.AppBindings)
		close(mockReader.FetchBindingsOutput.Err)
		mockPool := newMockAdapterPool()
		mockPool.ListOutput.Bindings <- [][]*v1.Binding{{
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
		}}
		close(mockPool.ListOutput.Bindings)
		close(mockPool.ListOutput.Err)
		close(mockPool.CreateOutput.Err)
		close(mockPool.DeleteOutput.Err)

		o := app.NewOrchestrator(mockReader, mockPool)
		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Eventually(mockPool.DeleteInput.Binding).Should(HaveLen(1))

		var binding *v1.Binding
		Eventually(mockPool.DeleteInput.Binding).Should(Receive(&binding))
		Expect(binding).To(Equal(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://other-drain",
			},
		))
	})
})
