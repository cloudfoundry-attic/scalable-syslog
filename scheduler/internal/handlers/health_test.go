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
		recorder *httptest.ResponseRecorder
		health   *handlers.Health
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		health = handlers.NewHealth(newSpyCounter(5), newSpyCounter(1))
	})

	It("returns JSON body with drain count", func() {

		health.ServeHTTP(recorder, new(http.Request))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.Bytes()).To(MatchJSON(`{"drainCount": 5, "adapterCount": 1}`))
	})
})

func newSpyCounter(count int) *spyCounter {
	return &spyCounter{count: count}
}

type spyCounter struct {
	count int
}

func (s *spyCounter) Count() int {
	return s.count
}

func TestHandlers(t *testing.T) {
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduler - Handlers Suite")
}
