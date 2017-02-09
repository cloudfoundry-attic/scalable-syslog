package drainstore_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/drainstore"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cache", func() {
	var (
		cache *drainstore.Cache
	)

	BeforeEach(func() {
		cache = drainstore.NewCache()
	})

	It("keeps track of the drains", func() {
		cache.Add(&v1.Binding{
			AppId:    "some-id",
			Hostname: "some-hostname",
			Drain:    "some.url",
		})

		bindings := cache.List()

		Expect(bindings).To(HaveLen(1))
		Expect(bindings[0].AppId).To(Equal("some-id"))
		Expect(bindings[0].Hostname).To(Equal("some-hostname"))
		Expect(bindings[0].Drain).To(Equal("some.url"))
	})

	It("does not add duplicate bindings", func() {
		for i := 0; i < 2; i++ {
			cache.Add(&v1.Binding{
				AppId:    "some-id",
				Hostname: "some-hostname",
				Drain:    "some.url",
			})
		}

		bindings := cache.List()

		Expect(bindings).To(HaveLen(1))
	})
})
