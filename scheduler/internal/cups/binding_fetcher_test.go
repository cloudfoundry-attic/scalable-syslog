package cups_test

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cups"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BindingFetcher", func() {
	var (
		mockGetter *mockGetter
		fetcher    *cups.BindingFetcher
	)

	BeforeEach(func() {
		mockGetter = newMockGetter()
		fetcher = cups.NewBindingFetcher(mockGetter)
	})

	Context("when the getter does not return an error", func() {
		BeforeEach(func() {
			close(mockGetter.GetOutput.Err)
		})

		Context("when the status code is 200 and the body is valid json", func() {
			BeforeEach(func() {
				mockGetter.GetOutput.Resp <- &http.Response{
					StatusCode: http.StatusOK,
					Body: ioutil.NopCloser(strings.NewReader(`
					{
					  "results": {
						"9be15160-4845-4f05-b089-40e827ba61f1": {
						  "drains": [
							"syslog://some.url",
							"syslog://some.other.url"
						  ],
						  "hostname": "org.space.logspinner"
						}
					  },
					  "next_id": 50
					}
				`)),
				}

				mockGetter.GetOutput.Resp <- &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(strings.NewReader(`{ "results": { }, "next_id": null }`)),
				}
			})

			It("returns the bindings", func() {
				bindings, err := fetcher.FetchBindings()
				Expect(err).ToNot(HaveOccurred())
				Expect(bindings).To(HaveLen(1))

				appID := "9be15160-4845-4f05-b089-40e827ba61f1"
				Expect(bindings).To(HaveKey(appID))
				Expect(bindings[appID].Hostname).To(Equal("org.space.logspinner"))
				Expect(bindings[appID].Drains).To(ConsistOf("syslog://some.url", "syslog://some.other.url"))
			})

			It("fetches all the pages", func() {
				fetcher.FetchBindings()
				Expect(mockGetter.GetCalled).To(HaveLen(2))
				Expect(mockGetter.GetInput.NextID).To(Receive(Equal(0)))
				Expect(mockGetter.GetInput.NextID).To(Receive(Equal(50)))
			})
		})

		Context("when the status code is 200 and the body is invalid json", func() {
			BeforeEach(func() {
				mockGetter.GetOutput.Resp <- &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(strings.NewReader("invalid")),
				}
			})

			It("returns an error", func() {
				_, err := fetcher.FetchBindings()
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when the status code is not 200", func() {
			BeforeEach(func() {
				mockGetter.GetOutput.Resp <- &http.Response{StatusCode: http.StatusBadRequest}
			})

			It("returns an error", func() {
				_, err := fetcher.FetchBindings()
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("when the getter does returns an error", func() {
		BeforeEach(func() {
			mockGetter.GetOutput.Err <- errors.New("some-error")
			close(mockGetter.GetOutput.Resp)
		})

		It("returns an error", func() {
			_, err := fetcher.FetchBindings()
			Expect(err).To(HaveOccurred())
		})
	})

})
