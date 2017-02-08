package endtoend_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestEndtoend(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Endtoend Suite")
}
