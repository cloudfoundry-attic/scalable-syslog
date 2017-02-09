package drainstore_test

import (
	"log"
	"testing"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/cups"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/drainstore"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cache", func() {
	var (
		cache *drainstore.Cache
	)

	BeforeEach(func() {
		cache = drainstore.New()
	})

	It("updates the count according to the last state", func() {
		cache.StoreBindings(nil)
		Expect(cache.Count()).To(Equal(0))

		cache.StoreBindings(map[string]cups.Binding{
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

func TestDrainstore(t *testing.T) {
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduler - Drainstore Suite")
}
