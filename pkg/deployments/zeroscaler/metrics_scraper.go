package zeroscaler

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/dailymotion-oss/osiris/pkg/metrics"
)

type metricsScraperConfig struct {
	ScraperName    string          `json:"type"`
	Implementation json.RawMessage `json:"implementation"`
}

type metricsScraper interface {
	Scrape(pod *corev1.Pod) *metrics.ProxyRequestCount
}

func newMetricsScraper(config metricsScraperConfig) (metricsScraper, error) {
	var (
		scraper metricsScraper
		err     error
	)
	switch config.ScraperName {
	case prometheusScraperName:
		scraper, err = newPrometheusScraper(config)
	case osirisScraperName:
		scraper = newOsirisScraper()
	default:
		return nil, fmt.Errorf("unknown scraper %s", config.ScraperName)
	}
	return scraper, err
}
