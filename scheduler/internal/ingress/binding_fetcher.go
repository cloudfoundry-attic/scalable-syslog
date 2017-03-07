package ingress

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
)

// Getter is configured to fetch HTTP responses
type Getter interface {
	Get(nextID int) (resp *http.Response, err error)
}

// BindingFetcher uses a Getter to fetch and decode Bindings
type BindingFetcher struct {
	getter     Getter
	mu         sync.RWMutex
	drainCount int
}

// Binding reflects the JSON encoded output from the syslog drain binding provider
type Binding struct {
	Drains   []string
	Hostname string
}

type AppBindings map[string]Binding

type response struct {
	Results AppBindings
	NextID  int `json:"next_id"`
}

// NewBindingFetcher returns a new BindingFetcher
func NewBindingFetcher(g Getter) *BindingFetcher {
	return &BindingFetcher{
		getter: g,
	}
}

// FetchBindings reaches out to the syslog drain binding provider via the Getter and decodes
// the response. If it does not get a 200, it returns an error.
func (f *BindingFetcher) FetchBindings() (AppBindings, error) {
	drains := make(AppBindings)
	nextID := 0
	f.resetDrainCount()

	for {
		resp, err := f.getter.Get(nextID)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("received %d status code from syslog drain binding API", resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var r response
		if err = json.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("invalid API response body")
		}

		for appID, binding := range r.Results {
			drains[appID] = binding
			f.incrementDrainCount(len(binding.Drains))
		}

		if r.NextID == 0 {
			return drains, nil
		}
		nextID = r.NextID
	}
}

func (f *BindingFetcher) resetDrainCount() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.drainCount = 0
}

func (f *BindingFetcher) incrementDrainCount(c int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.drainCount += c
}

func (f *BindingFetcher) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.drainCount
}
