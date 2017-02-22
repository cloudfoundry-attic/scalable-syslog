package ingress

import (
	"net/url"
	"sync"
)

type BindingReader interface {
	FetchBindings() (appBindings AppBindings, err error)
}

// VersionFilter wraps a BindingReader and filters out versions that do not
// contain a drain-version query argument that matches.
type VersionFilter struct {
	br         BindingReader
	mu         sync.RWMutex
	drainCount int
}

// NewVersionFilter creates a new VersionFilter.
func NewVersionFilter(br BindingReader) *VersionFilter {
	return &VersionFilter{
		br: br,
	}
}

// FetchBindings calls the wrapped BindingReader and filters the result.
func (f *VersionFilter) FetchBindings() (AppBindings, error) {
	f.resetDrainCount()
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, err
	}
	newBindings := AppBindings{}
	for appID, b := range sourceBindings {
		drainURLs := []string{}
		for _, d := range b.Drains {
			url, err := url.Parse(d)
			if err != nil {
				continue
			}
			if url.Query().Get("drain-version") == "2.0" {
				f.incrementDrainCount(1)
				drainURLs = append(drainURLs, d)
			}
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

func (f *VersionFilter) resetDrainCount() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.drainCount = 0
}

func (f *VersionFilter) incrementDrainCount(c int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.drainCount += c
}

func (f *VersionFilter) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.drainCount
}
