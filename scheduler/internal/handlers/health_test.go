package handlers_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/handlers"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Health", func() {
	var (
		mockDrainCounter *mockDrainCounter
		recorder         *httptest.ResponseRecorder

		health *handlers.Health
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		mockDrainCounter = newMockDrainCounter()
		health = handlers.NewHealth(mockDrainCounter)
	})

	It("returns JSON body with drain count", func() {
		mockDrainCounter.CountOutput.Drains <- 5
		health.ServeHTTP(recorder, new(http.Request))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.Bytes()).To(MatchJSON(`{"drainCount": 5}`))
	})
})

func TestHandlers(t *testing.T) {
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduler - Handlers Suite")
}
