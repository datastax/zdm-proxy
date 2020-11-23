package metrics_test

import (
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
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

func TestHistogramMetricWithLabels_String(t *testing.T) {
	metricWithLabel := metrics.NewHistogramMetricWithLabels(
		"testHistogram",
		"Description 1234",
		[]float64{1, 2},
		map[string]string{
			"label_312": "value_321",
			"label_123": "value_123",
		},
	)

	stingRepresentation := metricWithLabel.String()
	require.Equal(t, "testHistogram{label_123=\"value_123\",label_312=\"value_321\"}", stingRepresentation)
}

func TestHistogramMetric_String(t *testing.T) {
	metricWithLabel := metrics.NewHistogramMetric(
		"testHistogram",
		"Description 1234",
		[]float64{1, 2},
	)

	stingRepresentation := metricWithLabel.String()
	require.Equal(t, "testHistogram", stingRepresentation)
}