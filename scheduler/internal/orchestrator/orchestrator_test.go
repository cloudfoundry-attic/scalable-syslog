package orchestrator_test

import (
	"testing"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/orchestrator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {

	It("returns the number of adapters", func() {
		adapters := []string{"1.2.3.4:1234"}
		o := orchestrator.New(adapters)

		Expect(o.Count()).To(Equal(1))
	})

})

func TestOrchestrator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Orchestrator Suite")
}
