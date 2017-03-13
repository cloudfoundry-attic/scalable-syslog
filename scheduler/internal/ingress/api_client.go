package ingress

import (
	"fmt"
	"net/http"
)

type APIClient struct {
	Client *http.Client
	Addr   string
}

func (w APIClient) Get(nextID int) (*http.Response, error) {
	return w.Client.Get(fmt.Sprintf("%s/internal/v4/syslog_drain_urls?batch_size=50&next_id=%d", w.Addr, nextID))
}
