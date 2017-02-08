package drainstore_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cupsprovider"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/drainstore"

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

	It("updates the count according to the last state", func() {
		cache.StoreBindings(nil)
		Expect(cache.Count()).To(Equal(0))

		cache.StoreBindings(map[string]cupsprovider.Binding{
			"a": {
				Hostname: "some-hostname",
				Drains: []string{
					"some-drain",
					"some-other-drain",
				},
			},
			"b": {
				Hostname: "some-hostname",
				Drains: []string{
					"some-drain",
					"some-other-drain",
				},
			},
		})

		Expect(cache.Count()).To(Equal(4))
	})
})
