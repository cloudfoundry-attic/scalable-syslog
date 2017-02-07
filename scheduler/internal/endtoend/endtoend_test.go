package endtoend_test

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Endtoend", func() {
	var (
		schedulerAddr string
	)

	BeforeEach(func() {
		schedulerAddr = app.StartScheduler("localhost:0")
	})

	It("reports the number of drains", func() {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", schedulerAddr))
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())
		Expect(body).To(MatchJSON(`{"drainCount": 0}`))
	})

})
