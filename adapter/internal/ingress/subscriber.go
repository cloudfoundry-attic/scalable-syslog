package ingress

import (
	"context"
	"log"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

type ClientPool interface {
	Next() (client v2.EgressClient)
}

type WriterBuilder interface {
	Build(binding *v1.Binding) (cw egress.WriteCloser, err error)
}

// Subscriber streams loggregator egress to the syslog drain.
type Subscriber struct {
	pool    ClientPool
	builder WriterBuilder
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(cp ClientPool, wb WriterBuilder) *Subscriber {
	return &Subscriber{
		pool:    cp,
		builder: wb,
	}
}

// Start begins to stream logs from a loggregator egress client to the syslog
// egress writer. Start does not block.
func (s *Subscriber) Start(binding *v1.Binding) {
	go func() {
		for {
			writer, err := s.builder.Build(binding)
			if err != nil {
				log.Println("failed to build egress writer: %s", err)
				return
			}

			client := s.pool.Next()
			receiver, err := client.Receiver(context.Background(), &v2.EgressRequest{
				ShardId: buildShardId(binding),
				Filter:  &v2.Filter{SourceId: binding.AppId},
			})
			if err != nil {
				continue
			}

			readWriteLoop(receiver, writer)
		}
	}()
}

func readWriteLoop(r v2.Egress_ReceiverClient, w egress.WriteCloser) error {
	defer r.CloseSend()
	defer w.Close()

	for {
		env, err := r.Recv()
		if err != nil {
			return err
		}
		w.Write(env)
	}
}

func buildShardId(binding *v1.Binding) (key string) {
	return binding.AppId + binding.Hostname + binding.Drain
}
