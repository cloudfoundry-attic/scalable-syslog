package cupsprovider_test

import (
	"errors"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cupsprovider"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Poller", func() {
	var (
		mockProvider *mockProvider
		mockStore    *mockStore
	)

	BeforeEach(func() {
		mockProvider = newMockProvider()
		mockStore = newMockStore()

		cupsprovider.StartPoller(time.Millisecond, mockProvider, mockStore)
	})

	Context("when the provider does not return an error", func() {
		BeforeEach(func() {
			mockProvider.FetchBindingsOutput.Bindings <- map[string]cupsprovider.Binding{
				"A": cupsprovider.Binding{
					Hostname: "some-hostname",
				},
			}
			close(mockProvider.FetchBindingsOutput.Err)
		})

		It("periodically polls the provider and stores the result", func() {
			var bindings map[string]cupsprovider.Binding
			Eventually(mockStore.StoreBindingsInput.Bindings).Should(Receive(&bindings))
			Expect(bindings).To(HaveKeyWithValue("A", cupsprovider.Binding{
				Hostname: "some-hostname",
			}))
		})
	})

	Context("when the provider returns an error", func() {
		BeforeEach(func() {
			close(mockProvider.FetchBindingsOutput.Bindings)
			mockProvider.FetchBindingsOutput.Err <- errors.New("some-error")
		})

		It("retries", func() {
			Eventually(mockProvider.FetchBindingsCalled).Should(HaveLen(2))
			Expect(mockStore.StoreBindingsCalled).To(HaveLen(0))
		})
	})
})
