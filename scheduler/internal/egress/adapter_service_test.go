package egress_test

import (
	"errors"

	"golang.org/x/net/context"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/egress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AdapterService", func() {
	Describe("List", func() {
		It("gets a list of de-duped bindings from all adapters", func() {
			client1 := &spyClient{}
			client2 := &spyClient{}
			client3 := &spyClient{}
			comm := newSpyCommunicator()

			binding := &v1.Binding{
				AppId:    "app-id",
				Hostname: "hostname",
				Drain:    "drain",
			}

			comm.listResults = map[interface{}][]interface{}{
				client1: []interface{}{
					binding,
					binding,
				},
				client2: []interface{}{
					binding,
				},
			}

			comm.listErrs = map[interface{}]error{
				client3: errors.New("some-error"),
			}

			s := egress.NewAdapterService(egress.AdapterPool{
				"test-addr-1": client1,
				"test-addr-2": client2,
				"test-addr-3": client3,
			}, comm)

			state := s.List()

			Expect(state["test-addr-1"]).To(HaveLen(1))
			Expect(state["test-addr-2"]).To(HaveLen(1))
			Expect(state["test-addr-3"]).To(HaveLen(0))

			Expect(state["test-addr-1"][0]).To(Equal(*binding))
			Expect(state["test-addr-2"][0]).To(Equal(*binding))
		})
	})

	Describe("Transition", func() {
		It("does nothing if both states are the same", func() {
			comm := newSpyCommunicator()
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := actual

			client := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client}, comm)
			s.Transition(actual, desired)
			Expect(comm.adds).To(BeEmpty())
			Expect(comm.removes).To(BeEmpty())
		})

		It("adds bindings for new adapters", func() {
			actual := egress.State{}
			desired := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			comm := newSpyCommunicator()
			client := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client}, comm)
			s.Transition(actual, desired)

			Expect(comm.adds[client]).To(ConsistOf(
				&v1.Binding{
					AppId:    "app-id",
					Hostname: "hostname",
					Drain:    "drain-url",
				},
			))
			Expect(comm.removes).To(BeEmpty())
		})

		It("deletes bindings for adapters that no longer exist", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{}

			comm := newSpyCommunicator()
			client := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client}, comm)
			s.Transition(actual, desired)

			Expect(comm.adds).To(BeEmpty())
			Expect(comm.removes[client]).To(ConsistOf(
				&v1.Binding{
					AppId:    "app-id",
					Hostname: "hostname",
					Drain:    "drain-url",
				},
			))
		})

		It("adds new bindings for existing adapters", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			comm := newSpyCommunicator()
			client := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client}, comm)
			s.Transition(actual, desired)

			Expect(comm.adds[client]).To(ConsistOf(
				&v1.Binding{
					AppId:    "app-id-2",
					Hostname: "hostname",
					Drain:    "drain-url",
				},
			))
			Expect(comm.removes).To(BeEmpty())
		})

		It("deletes old bindings for existing adapters", func() {
			actual := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{
				"test-addr": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			comm := newSpyCommunicator()
			client := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{"test-addr": client}, comm)
			s.Transition(actual, desired)

			Expect(comm.adds).To(BeEmpty())
			Expect(comm.removes[client]).To(ConsistOf(
				&v1.Binding{
					AppId:    "app-id-2",
					Hostname: "hostname",
					Drain:    "drain-url",
				},
			))
		})

		It("can move a binding from one adapter to another", func() {
			actual := egress.State{
				"test-addr-1": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
				"test-addr-2": []v1.Binding{
					{
						AppId:    "app-id-3",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}
			desired := egress.State{
				"test-addr-1": []v1.Binding{
					{
						AppId:    "app-id-1",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
				"test-addr-2": []v1.Binding{
					{
						AppId:    "app-id-2",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
					{
						AppId:    "app-id-3",
						Hostname: "hostname",
						Drain:    "drain-url",
					},
				},
			}

			comm := newSpyCommunicator()
			client1 := &spyClient{}
			client2 := &spyClient{}
			s := egress.NewAdapterService(egress.AdapterPool{
				"test-addr-1": client1,
				"test-addr-2": client2,
			}, comm)
			s.Transition(actual, desired)

			Expect(comm.adds[client2]).To(ConsistOf(
				&v1.Binding{
					AppId:    "app-id-2",
					Hostname: "hostname",
					Drain:    "drain-url",
				},
			))
			Expect(comm.removes[client1]).To(ConsistOf(
				&v1.Binding{
					AppId:    "app-id-2",
					Hostname: "hostname",
					Drain:    "drain-url",
				},
			))
		})
	})
})

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
