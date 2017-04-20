package metric

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/scalable-syslog/internal/diodes"
	gendiodes "github.com/cloudfoundry/diodes"

	"github.com/cloudfoundry-incubator/scalable-syslog/internal/api/loggregator/v2"

	"google.golang.org/grpc"
)

type Emitter struct {
	consumerAddr  string
	dialOpts      []grpc.DialOption
	sourceID      string
	batchInterval time.Duration
	tags          map[string]string

	client      loggregator_v2.IngressClient
	batchBuffer *diodes.ManyToOneEnvelopeV2

	mu sync.RWMutex
	s  loggregator_v2.Ingress_SenderClient
}

type EmitterOpt func(*Emitter)

func WithGrpcDialOpts(opts ...grpc.DialOption) func(e *Emitter) {
	return func(e *Emitter) {
		e.dialOpts = opts
	}
}

func WithSourceID(id string) func(e *Emitter) {
	return func(e *Emitter) {
		e.sourceID = id
	}
}

func WithAddr(addr string) func(e *Emitter) {
	return func(e *Emitter) {
		e.consumerAddr = addr
	}
}

func WithBatchInterval(interval time.Duration) func(e *Emitter) {
	return func(e *Emitter) {
		e.batchInterval = interval
	}
}

func WithOrigin(name string) func(e *Emitter) {
	return func(e *Emitter) {
		e.tags["origin"] = name
	}
}

func WithDeploymentMeta(deployment, job, index string) func(e *Emitter) {
	return func(e *Emitter) {
		e.tags["deployment"] = deployment
		e.tags["job"] = job
		e.tags["index"] = index
	}
}

func New(opts ...EmitterOpt) (*Emitter, error) {
	e := &Emitter{
		consumerAddr:  "localhost:3458",
		dialOpts:      []grpc.DialOption{grpc.WithInsecure()},
		batchInterval: 10 * time.Second,
		tags:          map[string]string{"prefix": "loggregator"},
	}

	for _, opt := range opts {
		opt(e)
	}
	e.dialOpts = append(e.dialOpts, grpc.WithBackoffMaxDelay(time.Second))

	e.batchBuffer = diodes.NewManyToOneEnvelopeV2(1000, gendiodes.AlertFunc(func(missed int) {
		log.Printf("dropped metrics %d", missed)
	}))

	conn, err := grpc.Dial(e.consumerAddr, e.dialOpts...)
	if err != nil {
		return nil, err
	}

	e.client = loggregator_v2.NewIngressClient(conn)
	e.s, err = e.client.Sender(context.Background())
	if err != nil {
		log.Printf("Failed to get sender from metric consumer: %s", err)
	}

	go e.runBatcher()
	go e.maintainer()

	return e, nil
}

func (e *Emitter) runBatcher() {
	ticker := time.NewTicker(e.batchInterval)
	defer ticker.Stop()

	for range ticker.C {
		s := e.sender()

		if s == nil {
			continue
		}

		for _, envelope := range e.aggregateCounters() {
			s.Send(envelope)
		}
	}
}

func (e *Emitter) aggregateCounters() map[string]*loggregator_v2.Envelope {
	m := make(map[string]*loggregator_v2.Envelope)
	for {
		envelope, ok := e.batchBuffer.TryNext()
		if !ok {
			break
		}

		// BUG: we need to batch by tags as well as name
		existingEnvelope, ok := m[envelope.GetCounter().Name]
		if !ok {
			m[envelope.GetCounter().Name] = envelope
			continue
		}

		value := existingEnvelope.GetCounter().GetValue().(*loggregator_v2.Counter_Delta)
		value.Delta += envelope.GetCounter().GetDelta()
	}
	return m
}

func (e *Emitter) maintainer() {
	for range time.Tick(time.Second) {
		s := e.sender()

		if s != nil {
			continue
		}

		s, err := e.client.Sender(context.Background())
		if err != nil {
			log.Printf("Failed to get sender from metric consumer: %s (retrying)", err)
			continue
		}

		e.setSender(s)
	}
}

func (e *Emitter) sender() loggregator_v2.Ingress_SenderClient {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.s
}

func (e *Emitter) setSender(s loggregator_v2.Ingress_SenderClient) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.s = s
}

type IncrementOpt func(*incrementOption)

type incrementOption struct {
	delta uint64
	tags  map[string]string
}

func WithIncrement(delta uint64) func(*incrementOption) {
	return func(i *incrementOption) {
		i.delta = delta
	}
}

func WithVersion(major, minor uint) func(*incrementOption) {
	return func(i *incrementOption) {
		i.tags["metric_version"] = fmt.Sprintf("%d.%d", major, minor)
	}
}

func WithTag(name, value string) func(*incrementOption) {
	return func(i *incrementOption) {
		i.tags[name] = value
	}
}

func (e *Emitter) IncCounter(name string, options ...IncrementOpt) {
	if e.batchBuffer == nil {
		return
	}

	incConf := &incrementOption{
		delta: 1,
		tags:  make(map[string]string),
	}

	for _, opt := range options {
		opt(incConf)
	}

	tags := v2Tags(incConf.tags)
	etags := v2Tags(e.tags)
	for k, v := range etags {
		tags[k] = v
	}

	envelope := &loggregator_v2.Envelope{
		SourceId:  e.sourceID,
		Timestamp: time.Now().UnixNano(),
		Message: &loggregator_v2.Envelope_Counter{
			Counter: &loggregator_v2.Counter{
				Name: name,
				Value: &loggregator_v2.Counter_Delta{
					Delta: incConf.delta,
				},
			},
		},
		Tags: tags,
	}

	e.batchBuffer.Set(envelope)
}

func v2Tags(tags map[string]string) map[string]*loggregator_v2.Value {
	v2tags := make(map[string]*loggregator_v2.Value)
	for k, v := range tags {
		v2tags[k] = &loggregator_v2.Value{
			Data: &loggregator_v2.Value_Text{
				Text: v,
			},
		}
	}
	return v2tags
}
