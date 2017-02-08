package cupsprovider

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// Getter is configured to fetch HTTP responses
type Getter interface {
	Get() (resp *http.Response, err error)
}

// BindingFetcher uses a Getter to fetch and decode Bindings
type BindingFetcher struct {
	getter Getter
}

// Binding reflects the JSON encoded output from the CUPS provider
type Binding struct {
	Drains   []string
	Hostname string
}

type cupsResponse struct {
	Results map[string]Binding
}

// NewBindingFetcher returns a new BindingFetcher
func NewBindingFetcher(g Getter) *BindingFetcher {
	return &BindingFetcher{
		getter: g,
	}
}

// FetchBindings reaches out to the CUPS provider via the Getter and decodes
// the response. If it does not get a 200, it returns an error.
func (f *BindingFetcher) FetchBindings() (map[string]Binding, error) {
	resp, err := f.getter.Get()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received %d status code from CUPS provider", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var cupsResp cupsResponse
	if err = json.Unmarshal(body, &cupsResp); err != nil {
		return nil, fmt.Errorf("invalid CUPS response body")
	}

	return cupsResp.Results, nil
}
