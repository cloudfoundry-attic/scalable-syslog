package ingress

type BlacklistFilter struct {
	ranges *IPRanges
	br     BindingReader
}

func NewBlacklistFilter(r *IPRanges, b BindingReader) *BlacklistFilter {
	return &BlacklistFilter{
		ranges: r,
		br:     b,
	}
}

func (f *BlacklistFilter) FetchBindings() (AppBindings, error) {
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, err
	}
	newBindings := AppBindings{}
	for appID, b := range sourceBindings {
		drainURLs := []string{}
		for _, d := range b.Drains {
			ok, err := f.ranges.IpOutsideOfRanges(d)
			_ = err
			// TODO: err on URL and this
			if !ok {
				continue
			}
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
