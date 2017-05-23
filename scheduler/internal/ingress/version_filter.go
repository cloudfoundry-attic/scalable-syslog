package ingress

import (
	"net/url"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

type BindingReader interface {
	FetchBindings() (appBindings []v1.Binding, err error)
}

// VersionFilter wraps a BindingReader and filters out versions that do not
// contain a drain-version query argument that matches.
type VersionFilter struct {
	br BindingReader
}

// NewVersionFilter creates a new VersionFilter.
func NewVersionFilter(br BindingReader) *VersionFilter {
	return &VersionFilter{
		br: br,
	}
}

// FetchBindings calls the wrapped BindingReader and filters the result.
func (f *VersionFilter) FetchBindings() ([]v1.Binding, error) {
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, err
	}
	newBindings := []v1.Binding{}
	for _, binding := range sourceBindings {
		url, err := url.Parse(binding.Drain)
		if err != nil {
			continue
		}
		if url.Query().Get("drain-version") == "2.0" {
			newBindings = append(newBindings, binding)
		}
	}
	return newBindings, nil
}
