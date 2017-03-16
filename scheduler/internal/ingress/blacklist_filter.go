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

func (f *BlacklistFilter) FetchBindings() (Bindings, error) {
	f.resetDrainCount()
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, err
	}
	newBindings := Bindings{}
	for _, binding := range sourceBindings {
		err := f.ranges.IpOutsideOfRanges(binding.Drain)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		newBindings = append(newBindings, binding)
		f.incrementDrainCount(1)
	}

	return newBindings, nil
}

func (f *BlacklistFilter) resetDrainCount() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.drainCount = 0
}

func (f *BlacklistFilter) incrementDrainCount(c int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.drainCount += c
}

func (f *BlacklistFilter) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.drainCount
}
