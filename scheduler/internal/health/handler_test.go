package health_test

import (
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/health"
)

var _ = Describe("Health", func() {
	var (
		recorder *httptest.ResponseRecorder
		handler   *health.Health
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		handler = health.NewHealth(newSpyCounter(5), newSpyCounter(1))
	})

	It("returns JSON body with drain count", func() {
		handler.ServeHTTP(recorder, new(http.Request))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Header().Get("Content-Type")).To(
			Equal("application/json; charset=utf-8"),
		)
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
