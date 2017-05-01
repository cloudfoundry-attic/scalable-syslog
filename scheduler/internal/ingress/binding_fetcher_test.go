package ingress_test

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

var _ = Describe("BindingFetcher", func() {
	var (
		getter  *SpyGetter
		fetcher *ingress.BindingFetcher
	)

	BeforeEach(func() {
		getter = &SpyGetter{}
		fetcher = ingress.NewBindingFetcher(getter)
	})

	Context("when the getter does not return an error", func() {
		Context("when the status code is 200 and the body is valid json", func() {
			BeforeEach(func() {
				getter.getResponses = []*http.Response{
					&http.Response{
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
					},

					&http.Response{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader(`{ "results": { }, "next_id": null }`)),
					},
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
				Expect(getter.getCalled).To(Equal(2))
				Expect(getter.getNextID[0]).To(Equal(0))
				Expect(getter.getNextID[1]).To(Equal(50))
			})
		})

		Context("when the status code is 200 and the body is invalid json", func() {
			BeforeEach(func() {
				getter.getResponses = []*http.Response{
					&http.Response{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader("invalid")),
					},
				}
			})

			It("returns an error", func() {
				_, err := fetcher.FetchBindings()
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when the status code is not 200", func() {
			BeforeEach(func() {
				getter.getResponses = []*http.Response{{StatusCode: http.StatusBadRequest}}
			})

			It("returns an error", func() {
				_, err := fetcher.FetchBindings()
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("when the getter does returns an error", func() {
		It("returns an error", func() {
			getter.getResponses = []*http.Response{{StatusCode: 500}}
			getter.getError = errors.New("some-error")

			_, err := fetcher.FetchBindings()

			Expect(err).To(HaveOccurred())
		})
	})

})

type SpyGetter struct {
	currentResponse int
	getCalled       int
	getNextID       []int
	getResponses    []*http.Response
	getError        error
}

func (s *SpyGetter) Get(nextID int) (*http.Response, error) {
	s.getCalled++
	s.getNextID = append(s.getNextID, nextID)
	resp := s.getResponses[s.currentResponse]
	s.currentResponse++

	if s.getError != nil {
		return nil, s.getError
	}

	return resp, nil
}
