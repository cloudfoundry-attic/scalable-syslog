package ingress

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/egress"
	v2 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/internal/api/v1"
)

type ClientPool interface {
	Next() (client v2.EgressClient)
}

type SyslogConnector interface {
	Connect(binding *v1.Binding) (cw egress.WriteCloser, err error)
}

// Subscriber streams loggregator egress to the syslog drain.
type Subscriber struct {
	pool      ClientPool
	connector SyslogConnector
}

// NewSubscriber returns a new Subscriber.
func NewSubscriber(cp ClientPool, connector SyslogConnector) *Subscriber {
	return &Subscriber{
		pool:      cp,
		connector: connector,
	}
}

// Start begins to stream logs from a loggregator egress client to the syslog
// egress writer. Start does not block. Start returns a function that can be
// called to stop streaming logs.
func (s *Subscriber) Start(binding *v1.Binding) func() {
	var unsubscribe int32

	go s.connectAndRead(binding, &unsubscribe)

	return func() {
		atomic.AddInt32(&unsubscribe, 1)
	}
}

func (s *Subscriber) connectAndRead(binding *v1.Binding, unsubscribe *int32) {
	for {
		cont := s.attemptConnectAndRead(binding, unsubscribe)
		if !cont {
			return
		}
	}
}

func (s *Subscriber) attemptConnectAndRead(binding *v1.Binding, unsubscribe *int32) bool {
	writer, err := s.connector.Connect(binding)
	if err != nil {
		log.Println("Failed connecting to syslog: %s", err)
		return false
	}
	defer writer.Close()

	// TODO: What if we cannot get a client?
	client := s.pool.Next()
	receiver, err := client.Receiver(context.Background(), &v2.EgressRequest{
		ShardId: buildShardId(binding),
		Filter:  &v2.Filter{SourceId: binding.AppId},
	})
	if err != nil {
		return true
	}
	defer receiver.CloseSend()

	if err := s.readWriteLoop(receiver, writer, unsubscribe); err != nil {
		log.Printf("Subscriber read/write loop has unexpectedly closed: %s", err)
		return true
	}

	return false
}

func (s *Subscriber) readWriteLoop(r v2.Egress_ReceiverClient, w egress.WriteCloser, unsubscribe *int32) error {
	for {
		if atomic.LoadInt32(unsubscribe) > 0 {
			log.Print("Subscriber read/write loop is closing")
			return nil
		}

		env, err := r.Recv()
		if err != nil {
			return err
		}
		if env.GetLog() == nil {
			continue
		}

		// We decided to ignore the error from the writer since in most
		// situations the connector will provide a diode writer and the diode
		// writer never returns an error.
		_ = w.Write(env)
	}
}

func buildShardId(binding *v1.Binding) (key string) {
	return binding.AppId + binding.Hostname + binding.Drain
}
