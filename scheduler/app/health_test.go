package app_test

import (
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Health", func() {
	var (
		recorder *httptest.ResponseRecorder
		health   *app.Health
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		health = app.NewHealth(newSpyCounter(1), newSpyCounter(5))
	})

	It("returns JSON body with drain count", func() {
		health.ServeHTTP(recorder, new(http.Request))
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
