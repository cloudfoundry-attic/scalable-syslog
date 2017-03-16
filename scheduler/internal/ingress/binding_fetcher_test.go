package ingress_test

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

var _ = Describe("BindingFetcher", func() {
	var (
		mockGetter *mockGetter
		fetcher    *ingress.BindingFetcher
	)

	BeforeEach(func() {
		mockGetter = newMockGetter()
		fetcher = ingress.NewBindingFetcher(mockGetter)
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
				Expect(bindings).To(HaveLen(2))

				appID := "9be15160-4845-4f05-b089-40e827ba61f1"
				Expect(bindings).To(ContainElement(v1.Binding{
					AppId:    appID,
					Hostname: "org.space.logspinner",
					Drain:    "syslog://some.url",
				}))
				Expect(bindings).To(ContainElement(v1.Binding{
					AppId:    appID,
					Hostname: "org.space.logspinner",
					Drain:    "syslog://some.other.url",
				}))
			})

			It("fetches all the pages", func() {
				fetcher.FetchBindings()
				Expect(mockGetter.GetCalled).To(HaveLen(2))
				Expect(mockGetter.GetInput.NextID).To(Receive(Equal(0)))
				Expect(mockGetter.GetInput.NextID).To(Receive(Equal(50)))
			})

			It("reports the number of application syslog drains", func() {
				Expect(fetcher.Count()).To(Equal(0))
				fetcher.FetchBindings()
				Expect(fetcher.Count()).To(Equal(2))
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
