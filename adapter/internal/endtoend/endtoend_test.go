package endtoend_test

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/app"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Adapter - Endtoend", func() {
	var (
		adapterHealthAddr string
	)

	BeforeEach(func() {
		adapterHealthAddr = app.StartAdapter("localhost:0")
	})

	It("reports the number of drains", func() {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", adapterHealthAddr))
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(body).To(MatchJSON(`{"drainCount": 0}`))
	})
})
