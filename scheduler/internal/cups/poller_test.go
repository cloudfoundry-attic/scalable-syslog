package cups_test

import (
	"errors"
	"log"
	"testing"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cups"

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

		cups.StartPoller(time.Millisecond, mockProvider, mockStore)
	})

	Context("when the provider does not return an error", func() {
		BeforeEach(func() {
			mockProvider.FetchBindingsOutput.Bindings <- map[string]cups.Binding{
				"A": cups.Binding{
					Hostname: "some-hostname",
				},
			}
			close(mockProvider.FetchBindingsOutput.Err)
		})

		It("periodically polls the provider and stores the result", func() {
			var bindings map[string]cups.Binding
			Eventually(mockStore.StoreBindingsInput.Bindings).Should(Receive(&bindings))
			Expect(bindings).To(HaveKeyWithValue("A", cups.Binding{
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

func TestCups(t *testing.T) {
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduler - Cups Suite")
}
