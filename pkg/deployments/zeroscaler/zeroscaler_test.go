package zeroscaler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	k8s "github.com/dailymotion-oss/osiris/pkg/kubernetes"
)

func TestGetMetricsScraperConfig(t *testing.T) {
	tests := []struct {
		name           string
		annotations    map[string]string
		expectedResult metricsScraperConfig
	}{
		{
			name: "use osiris scraper as the default value",
			expectedResult: metricsScraperConfig{
				ScraperName: osirisScraperName,
			},
		},
		{
			name: "custom annotation for osiris",
			annotations: map[string]string{
				k8s.MetricsCollectorAnnotationName: `
						{
							"type": "osiris"
						}
						`,
			},
			expectedResult: metricsScraperConfig{
				ScraperName: osirisScraperName,
			},
		},
		{
			name: "custom annotation for prometheus",
			annotations: map[string]string{
				k8s.MetricsCollectorAnnotationName: `
						{
							"type": "prometheus"
						}
						`,
			},
			expectedResult: metricsScraperConfig{
				ScraperName: prometheusScraperName,
			},
		},
		{
			name: "custom annotation for prometheus with specific impl",
			annotations: map[string]string{
				k8s.MetricsCollectorAnnotationName: `
						{
							"type": "prometheus",
							"implementation": {
								"port": 8080,
								"path": "/metrics",
								"requestCountMetricName": "requests",
								"requestCountMetricLabels": {
									"label": "key"
								}
							}
						}
						`,
			},
			expectedResult: metricsScraperConfig{
				ScraperName: prometheusScraperName,
				Implementation: []byte(`
				{
					"port": 8080,
					"path": "/metrics",
					"requestCountMetricName": "requests",
					"requestCountMetricLabels": {
						"label": "key"
					}
				}
				`),
			},
		},
		{
			name: "non-json content should fall back to osiris",
			annotations: map[string]string{
				k8s.MetricsCollectorAnnotationName: "some non-json content",
			},
			expectedResult: metricsScraperConfig{
				ScraperName: osirisScraperName,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := getMetricsScraperConfig("Deployment", "whatever", test.annotations)

			assert.Equal(t, test.expectedResult.ScraperName, actual.ScraperName)
			if len(test.expectedResult.Implementation) > 0 {
				assert.JSONEq(
					t,
					string(test.expectedResult.Implementation),
					string(actual.Implementation),
				)
			}
		})
	}
}
