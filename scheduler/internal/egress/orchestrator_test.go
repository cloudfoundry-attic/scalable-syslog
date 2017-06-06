package egress_test

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter/testhelper"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	It("provides actual state to the adapter service", func() {
		actualState := egress.State{
			"test-addr": []v1.Binding{
				{
					AppId:    "app-id",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			},
		}
		adapterService := &SpyAdapterService{
			actualState: actualState,
		}

		o := egress.NewOrchestrator(
			nil,
			&SpyReader{},
			adapterService,
			&SpyHealthEmitter{},
			testhelper.NewMetricClient(),
		)
		go o.Run(time.Millisecond)

		Eventually(adapterService.lastActualTransition).Should(Equal(actualState))
	})

	DescribeTable("it evenly distributes bindings across adapter addrs", func(adapterCount, bindingCount int, dist []int) {
		adapterService := &SpyAdapterService{}

		var (
			addrs    []string
			bindings []v1.Binding
		)
		for i := 0; i < adapterCount; i++ {
			addrs = append(addrs, fmt.Sprintf("test-addr-%d", i))
		}
		for i := 0; i < bindingCount; i++ {
			bindings = append(bindings, v1.Binding{
				AppId:    fmt.Sprintf("app-id-%d", i),
				Drain:    "syslog://my-app-drain",
				Hostname: "org.space.app",
			})
		}

		o := egress.NewOrchestrator(
			addrs,
			&SpyReader{drains: bindings},
			adapterService,
			&SpyHealthEmitter{},
			testhelper.NewMetricClient(),
		)
		go o.Run(time.Millisecond)

		var desired egress.State
		f := func() egress.State {
			desired = adapterService.lastDesiredTransition()
			return desired
		}
		Eventually(f).ShouldNot(BeNil())

		Expect(distribution(desired)).To(Equal(dist))
	},
		Entry("three adapters, three bindings", 3, 3, []int{2, 2, 2}),
		Entry("three adapters, two bindings", 3, 2, []int{1, 1, 2}),
		Entry("three adapters, four bindings", 3, 4, []int{2, 3, 3}),
		Entry("four adapters, two bindings", 4, 2, []int{1, 1, 1, 1}),
	)

	It("provides the same desired every time", func() {
		addrs := []string{
			"test-addr-1",
			"test-addr-2",
			"test-addr-3",
		}
		bindings := []v1.Binding{
			{
				AppId:    "app-id-1",
				Drain:    "syslog://my-app-drain",
				Hostname: "org.space.app",
			},
			{
				AppId:    "app-id-2",
				Drain:    "syslog://my-app-drain",
				Hostname: "org.space.app",
			},
		}
		adapterService := &SpyAdapterService{}

		o := egress.NewOrchestrator(
			addrs,
			&SpyReader{drains: bindings},
			adapterService,
			&SpyHealthEmitter{},
			testhelper.NewMetricClient(),
		)
		go o.Run(time.Millisecond)

		var desiredA egress.State
		f := func() egress.State {
			desiredA = adapterService.lastDesiredTransition()
			return desiredA
		}
		Eventually(f).ShouldNot(BeNil())

		var desiredB egress.State
		f = func() egress.State {
			desiredB = adapterService.lastDesiredTransition()
			return desiredB
		}
		Eventually(f).ShouldNot(BeNil())

		Expect(desiredA).To(Equal(desiredB))
	})

	It("does not duplicate binding on a single adapter", func() {
		addrs := []string{
			"test-addr-1",
		}
		reader := &SpyReader{
			drains: []v1.Binding{
				{
					AppId:    "app-id-1",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			},
		}
		adapterService := &SpyAdapterService{}

		o := egress.NewOrchestrator(
			addrs,
			reader,
			adapterService,
			&SpyHealthEmitter{},
			testhelper.NewMetricClient(),
		)
		go o.Run(time.Millisecond)

		Eventually(adapterService.lastDesiredTransition).Should(ConsistOf(
			BeEquivalentTo([]v1.Binding{
				{
					AppId:    "app-id-1",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			}),
		))
	})

	It("provides an empty desired state with no adapters", func() {
		reader := &SpyReader{
			drains: []v1.Binding{
				{
					AppId:    "app-id-1",
					Drain:    "syslog://my-app-drain",
					Hostname: "org.space.app",
				},
			},
		}
		adapterService := &SpyAdapterService{}

		o := egress.NewOrchestrator(
			nil,
			reader,
			adapterService,
			&SpyHealthEmitter{},
			testhelper.NewMetricClient(),
		)
		go o.Run(time.Millisecond)

		Eventually(adapterService.lastDesiredTransition).Should(BeEmpty())
	})

	It("does not write when the read fails", func() {
		reader := &SpyReader{
			err: errors.New("Nope!"),
		}
		adapterService := &SpyAdapterService{}

		o := egress.NewOrchestrator(
			nil,
			reader,
			adapterService,
			&SpyHealthEmitter{},
			testhelper.NewMetricClient(),
		)
		go o.Run(time.Millisecond)

		Consistently(adapterService.transitionCalled).Should(BeFalse())
	})
})

type SpyReader struct {
	drains []v1.Binding
	err    error
}

func (s *SpyReader) FetchBindings() (appBindings []v1.Binding, invalid int, err error) {
	return s.randomizeOrder(s.drains), 0, s.err
}

func (s *SpyReader) randomizeOrder(b []v1.Binding) []v1.Binding {
	var result []v1.Binding
	for _, n := range rand.Perm(len(b)) {
		result = append(result, b[n])
	}
	return result
}

type transition struct {
	actual  egress.State
	desired egress.State
}

type SpyAdapterService struct {
	actualState egress.State

	mu           sync.Mutex
	transitions_ []transition
}

func (s *SpyAdapterService) Transition(a, d egress.State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitions_ = append(s.transitions_, transition{a, d})
}

func (s *SpyAdapterService) transitionCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.transitions_) > 0
}

func (s *SpyAdapterService) lastTransition() transition {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.transitions_) < 1 {
		return transition{}
	}

	return s.transitions_[len(s.transitions_)-1]
}

func (s *SpyAdapterService) lastActualTransition() egress.State {
	t := s.lastTransition()
	return t.actual
}

func (s *SpyAdapterService) lastDesiredTransition() egress.State {
	t := s.lastTransition()
	return t.desired
}

func (s *SpyAdapterService) List() egress.State {
	return s.actualState
}

type SpyHealthEmitter struct{}

func (s *SpyHealthEmitter) SetCounter(map[string]int) {}

func countAdaptersWithBinding(b v1.Binding, s egress.State) int {
	var count int
	for _, bindings := range s {
		for _, binding := range bindings {
			if binding == b {
				count++
				break
			}
		}
	}
	return count
}

func distribution(s egress.State) []int {
	var dist []int
	for _, bindings := range s {
		dist = append(dist, len(bindings))
	}
	sort.Ints(dist)
	return dist
}
