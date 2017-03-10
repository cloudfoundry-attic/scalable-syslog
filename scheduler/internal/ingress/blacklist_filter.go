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

func (f *BlacklistFilter) FetchBindings() (AppBindings, error) {
	f.resetDrainCount()
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, err
	}
	newBindings := AppBindings{}
	for appID, b := range sourceBindings {
		drainURLs := []string{}
		for _, d := range b.Drains {
			err := f.ranges.IpOutsideOfRanges(d)
			if err != nil {
				log.Printf("%s", err)
				continue
			}
			f.incrementDrainCount(1)
			drainURLs = append(drainURLs, d)
		}
		if len(drainURLs) > 0 {
			binding := Binding{
				Hostname: b.Hostname,
				Drains:   drainURLs,
			}
			newBindings[appID] = binding
		}
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
