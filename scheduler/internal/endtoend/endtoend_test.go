package endtoend_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Endtoend", func() {
	var (
		schedulerAddr string

		cupsProvider *httptest.Server
	)

	BeforeEach(func() {
		cupsProvider = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`
					{
					  "results": {
						"9be15160-4845-4f05-b089-40e827ba61f1": {
						  "drains": [
							"syslog://some.url",
							"syslog://some.other.url"
						  ],
						  "hostname": "org.space.logspinner"
						}
					  }
					}
				`))
		}))

		schedulerAddr = app.StartScheduler(
			app.WithHealthAddr("localhost:0"),
			app.WithCUPSUrl(cupsProvider.URL),
			app.WithPollingInterval(time.Millisecond),
		)
	})

	It("reports the number of drains", func() {
		f := func() []byte {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", schedulerAddr))
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := ioutil.ReadAll(resp.Body)
			Expect(err).ToNot(HaveOccurred())
			return body
		}
		Eventually(f).Should(MatchJSON(`{"drainCount": 2}`))
	})

})
