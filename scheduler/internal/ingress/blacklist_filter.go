package ingress

import (
	"log"
	"sync"
)

type BlacklistFilter struct {
	ranges     *IPRanges
	br         BindingReader
	mu         sync.RWMutex
	drainCount int
}

func NewBlacklistFilter(r *IPRanges, b BindingReader) *BlacklistFilter {
	return &BlacklistFilter{
		ranges: r,
		br:     b,
	}
}

func (f *BlacklistFilter) FetchBindings() (Bindings, int, error) {
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, 0, err
	}
	newBindings := Bindings{}
	removed := 0

	for _, binding := range sourceBindings {
		err := f.ranges.IpOutsideOfRanges(binding.Drain)
		if err != nil {
			removed++
			log.Printf("%s", err)
			continue
		}
		newBindings = append(newBindings, binding)
	}

	return newBindings, removed, nil
}
