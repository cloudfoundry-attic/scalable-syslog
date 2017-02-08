package cupsprovider_test

//go:generate hel

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestCupsprovider(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Scheduler - CupsProvider Suite")
}
