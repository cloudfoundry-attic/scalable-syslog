package handlers_test

import (
	"net/http"
	"net/http/httptest"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/handlers"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Health", func() {
	var (
		mockStore *mockBindingStore
		handler   *handlers.Health
		recorder  *httptest.ResponseRecorder
	)

	BeforeEach(func() {
		mockStore = newMockBindingStore()
		handler = handlers.NewHealth(mockStore)
		recorder = httptest.NewRecorder()
	})

	It("returns the drain count", func() {
		mockStore.ListOutput.Bindings <- []*v1.Binding{nil, nil}

		handler.ServeHTTP(recorder, new(http.Request))

		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.Bytes()).To(MatchJSON(`{
			"drainCount": 2
		}`))
		Expect(recorder.Header().Get("Content-Type")).To(Equal("application/json"))
	})
})
