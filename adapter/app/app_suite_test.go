package app_test

import (
	"log"

	"google.golang.org/grpc/grpclog"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestEndtoend(t *testing.T) {
	grpclog.SetLogger(log.New(GinkgoWriter, "", log.LstdFlags))
	log.SetOutput(GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Adapter - App Suite")
}
