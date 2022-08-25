package metrics_test

import (
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetricWithLabels_String(t *testing.T) {
	metricWithLabel := metrics.NewMetricWithLabels(
		"testMetric",
		"Description 1234",
		map[string]string{
			"label_312": "value_321",
			"label_123": "value_123",
		},
	)

	stingRepresentation := metricWithLabel.String()
	require.Equal(t, "testMetric{label_123=\"value_123\",label_312=\"value_321\"}", stingRepresentation)
}

func TestMetric_String(t *testing.T) {
	metricWithLabel := metrics.NewMetric(
		"testMetric",
		"Description 1234",
	)

	stingRepresentation := metricWithLabel.String()
	require.Equal(t, "testMetric", stingRepresentation)
}