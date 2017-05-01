package health_test

import (
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/scalable-syslog/internal/health"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Health", func() {
	var (
		recorder *httptest.ResponseRecorder
		handler  *health.Health
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		handler = health.NewHealth()
	})

	It("returns JSON body with counts", func() {
		drainCounter := map[string]int{"drainCount": 5}
		adapterCounter := map[string]int{"adapterCount": 1}

		handler.SetCounter(drainCounter)
		handler.SetCounter(adapterCounter)

		handler.ServeHTTP(recorder, new(http.Request))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Header().Get("Content-Type")).To(
			Equal("application/json; charset=utf-8"),
		)
		Expect(recorder.Body.Bytes()).To(MatchJSON(`{"drainCount": 5, "adapterCount": 1}`))
	})

	It("can set a counter with multiple values", func() {
		multiCounter := map[string]int{
			"aCounter": 1,
			"bCounter": 2,
			"cCounter": 100,
		}

		handler.SetCounter(multiCounter)
		handler.ServeHTTP(recorder, new(http.Request))
		Expect(recorder.Body.Bytes()).To(MatchJSON(`{"aCounter": 1, "bCounter": 2, "cCounter": 100}`))
	})
})
