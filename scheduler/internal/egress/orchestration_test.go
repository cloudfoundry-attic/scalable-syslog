package egress_test

import (
	"errors"
	"math/rand"

	"context"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/testhelper"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestration", func() {
	var (
		// Scenarios
		listReturnErrs    func(count int)
		updateBindingList func([]v1.Binding)
		updateBindings    func([]v1.Binding, error)

		// Next term advances the orchestration. It must be invoked
		// to make a scenario take.
		nextTerm func()

		comm         *spyCommunicator
		adapterCount *testhelper.SpyMetric
	)

	BeforeEach(func() {
		client1 := &spyClient{}
		client2 := &spyClient{}
		client3 := &spyClient{}
		comm = newSpyCommunicator()

		mc := testhelper.NewMetricClient()

		bindingReader := &spyReader{}
		adapterPool := egress.AdapterPool{
			Pool: map[string]v1.AdapterClient{
				"test-addr-1": client1,
				"test-addr-2": client2,
				"test-addr-3": client3,
			},
		}
		orch := egress.NewOrchestrator(
			adapterPool,
			bindingReader,
			comm,
			&spyHealthEmitter{},
			mc,
		)
		nextTerm = orch.NextTerm
		adapterCount = mc.GetMetric("adapters")

		updateBindings = func(bs []v1.Binding, err error) {
			bindingReader.drains = bs
			bindingReader.err = err
		}

		updateBindingList = func(bs []v1.Binding) {
			clientList := []*spyClient{client1, client2, client3}
			comm.listResults = map[interface{}][]interface{}{}
			for i, b := range bs {
				comm.listResults[clientList[(i*2)%len(clientList)]] = append(
					comm.listResults[clientList[(i*2)%len(clientList)]],
					b,
				)
				comm.listResults[clientList[(i*2+1)%len(clientList)]] = append(
					comm.listResults[clientList[(i*2+1)%len(clientList)]],
					b,
				)
			}
		}

		listReturnErrs = func(count int) {
			clientList := []*spyClient{client1, client2, client3}
			comm.listErrs = map[interface{}]error{}
			for i := 0; i < count; i++ {
				comm.listErrs[clientList[i%len(clientList)]] = errors.New("some-error")
			}
		}
	})

	It("adds each drain to 2 unique adapters", func() {
		updateBindings([]v1.Binding{
			{AppId: "a"},
			{AppId: "b"},
			{AppId: "c"},
		}, nil)

		nextTerm()

		Expect(comm.adds).To(HaveLen(3))
		for _, bindings := range comm.adds {
			Expect(bindings).To(HaveLen(2))
			Expect(hasDuplicate(bindings)).To(BeFalse())
		}

		By("not changing anything if the binding order changes")
		updateBindings([]v1.Binding{
			{AppId: "b"},
			{AppId: "c"},
			{AppId: "a"},
		}, nil)

		nextTerm()
		Expect(comm.removes).To(HaveLen(0))
	})

	It("removes a binding", func() {
		updateBindingList([]v1.Binding{
			{AppId: "a"},
			{AppId: "b"},
		})

		updateBindings([]v1.Binding{
			{AppId: "a"},
		}, nil)

		nextTerm()

		Expect(comm.removes).To(HaveLen(2))
	})

	It("does not move bindings when binding reader returns an error", func() {
		updateBindings([]v1.Binding{
			{AppId: "a"},
			{AppId: "b"},
			{AppId: "c"},
		}, nil)

		nextTerm()

		addBefore := len(comm.adds)
		removeBefore := len(comm.removes)

		updateBindings([]v1.Binding{}, errors.New("some-error"))
		nextTerm()

		Expect(len(comm.adds)).To(Equal(addBefore))
		Expect(len(comm.removes)).To(Equal(removeBefore))
	})

	It("does not make changes if adapters return errors", func() {
		updateBindings([]v1.Binding{
			{AppId: "a"},
			{AppId: "b"},
			{AppId: "c"},
		}, nil)

		nextTerm()

		addBefore := len(comm.adds)
		removeBefore := len(comm.removes)

		listReturnErrs(3)
		nextTerm()

		Expect(len(comm.adds)).To(Equal(addBefore))
		Expect(len(comm.removes)).To(Equal(removeBefore))
	})

	It("does not write to an adapter that returns an error for list", func() {
		updateBindings([]v1.Binding{
			{AppId: "a"},
			{AppId: "b"},
		}, nil)
		listReturnErrs(1)

		nextTerm()

		Expect(comm.adds).To(HaveLen(2))
		Expect(adapterCount.GaugeValue()).To(Equal(float64(2)))
	})

	It("does not re-add a binding to an adapter", func() {
		updateBindings([]v1.Binding{
			{AppId: "a"},
		}, nil)
		listReturnErrs(2)

		// Do 2 terms and ensure only a single add happens
		nextTerm()
		nextTerm()

		Expect(comm.adds).To(HaveLen(1))
	})

	It("does not move drains when a new drain is added", func() {
		updateBindingList([]v1.Binding{
			{AppId: "a"},
			{AppId: "b"},
			{AppId: "c"},
		})

		updateBindings([]v1.Binding{
			{AppId: "d"},
		}, nil)

		Expect(comm.removes).To(HaveLen(0))
	})
})

func hasDuplicate(bindings []interface{}) bool {
	for i, b := range bindings {
		for j, bb := range bindings {
			if i != j && b.(v1.Binding) == bb.(v1.Binding) {
				return true
			}
		}
	}
	return false
}

type spyCommunicator struct {
	listResults map[interface{}][]interface{}
	listErrs    map[interface{}]error
	addsErr     map[interface{}]error
	removesErr  map[interface{}]error
	adds        map[interface{}][]interface{}
	removes     map[interface{}][]interface{}
}

type spyClient struct {
	v1.AdapterClient
}

func newSpyCommunicator() *spyCommunicator {
	return &spyCommunicator{
		adds:    make(map[interface{}][]interface{}),
		removes: make(map[interface{}][]interface{}),
	}
}

func (s *spyCommunicator) List(ctx context.Context, adapter interface{}) ([]interface{}, error) {
	return s.listResults[adapter], s.listErrs[adapter]
}

func (s *spyCommunicator) Add(ctx context.Context, worker, task interface{}) error {
	s.adds[worker] = append(s.adds[worker], task)
	return s.addsErr[worker]
}

func (s *spyCommunicator) Remove(ctx context.Context, worker, task interface{}) error {
	s.removes[worker] = append(s.removes[worker], task)
	return s.removesErr[worker]
}

type spyReader struct {
	drains []v1.Binding
	err    error
}

func (s *spyReader) FetchBindings() (appBindings []v1.Binding, invalid int, err error) {
	return s.randomizeOrder(s.drains), 0, s.err
}

func (s *spyReader) randomizeOrder(b []v1.Binding) []v1.Binding {
	var result []v1.Binding
	for _, n := range rand.Perm(len(b)) {
		result = append(result, b[n])
	}
	return result
}
