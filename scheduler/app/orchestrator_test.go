package app_test

import (
	"errors"
	"time"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	It("writes syslog bindings to the writer", func() {
		testReader := &spyReader{
			bindings: app.AppBindings{
				"app-id": app.Binding{
					Hostname: "org.space.app",
					Drains:   []string{"syslog://my-app-drain", "syslog://other-drain"},
				},
			},
		}
		testWriter := NewSpyWriter()
		o := app.NewOrchestrator(testReader, testWriter)

		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Eventually(testWriter.ActualWrites, 2).Should(Receive(Equal(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://my-app-drain",
			},
		)))
		Eventually(testWriter.ActualWrites, 2).Should(Receive(Equal(
			&v1.Binding{
				AppId:    "app-id",
				Hostname: "org.space.app",
				Drain:    "syslog://other-drain",
			},
		)))
	})

	It("does not write when the read fails", func() {
		testReader := &spyReader{
			bindings: app.AppBindings{
				"app-id": app.Binding{
					Hostname: "org.space.app",
					Drains:   []string{"syslog://my-app-drain", "syslog://other-drain"},
				},
			},
			err: errors.New("Nope!"),
		}
		testWriter := NewSpyWriter()
		o := app.NewOrchestrator(testReader, testWriter)

		go o.Run(1 * time.Millisecond)
		defer o.Stop()

		Consistently(testWriter.ActualWrites).ShouldNot(Receive())
	})
})

type spyReader struct {
	bindings app.AppBindings
	err      error
}

func (s *spyReader) FetchBindings() (app.AppBindings, error) {
	return s.bindings, s.err
}

func NewSpyWriter() *spyWriter {
	return &spyWriter{
		ActualWrites: make(chan *v1.Binding, 10),
	}
}

type spyWriter struct {
	ActualWrites chan *v1.Binding
}

func (s *spyWriter) Write(b *v1.Binding) error {
	s.ActualWrites <- b
	return nil
}
