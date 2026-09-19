package metrics_test

import (
	"testing"

	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics/prommetrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGetOrCreateWriteSuccessCounter(t *testing.T) {
	registry := prometheus.NewRegistry()
	factory := prommetrics.NewPrometheusMetricFactory(registry, "zdm")
	handler := metrics.NewMetricHandler(factory, nil, nil, nil, nil, nil, nil, nil)

	// Create a counter for origin/ks1/users
	counter1, err := handler.GetOrCreateWriteSuccessCounter("origin", "ks1", "users")
	require.NoError(t, err)
	require.NotNil(t, counter1)

	// Increment it
	counter1.Add(1)

	// Getting the same combination should return the same counter (cached)
	counter1Again, err := handler.GetOrCreateWriteSuccessCounter("origin", "ks1", "users")
	require.NoError(t, err)
	require.Equal(t, counter1, counter1Again)

	// Different table should return a different counter
	counter2, err := handler.GetOrCreateWriteSuccessCounter("origin", "ks1", "events")
	require.NoError(t, err)
	require.NotNil(t, counter2)

	// Different cluster same table should return a different counter
	counter3, err := handler.GetOrCreateWriteSuccessCounter("target", "ks1", "users")
	require.NoError(t, err)
	require.NotNil(t, counter3)

	// Verify counters are independent — increment counter2 and counter3
	counter2.Add(3)
	counter3.Add(5)

	// Gather metrics from the registry and verify the values
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	found := map[string]float64{}
	for _, mf := range metricFamilies {
		if mf.GetName() == "zdm_proxy_write_success_total" {
			for _, m := range mf.GetMetric() {
				labels := map[string]string{}
				for _, l := range m.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				key := labels["cluster"] + ":" + labels["keyspace"] + "." + labels["table"]
				found[key] = m.GetCounter().GetValue()
			}
		}
	}

	require.Equal(t, float64(1), found["origin:ks1.users"])
	require.Equal(t, float64(3), found["origin:ks1.events"])
	require.Equal(t, float64(5), found["target:ks1.users"])
}

func TestGetOrCreateWriteSuccessCounter_ConcurrentAccess(t *testing.T) {
	registry := prometheus.NewRegistry()
	factory := prommetrics.NewPrometheusMetricFactory(registry, "zdm")
	handler := metrics.NewMetricHandler(factory, nil, nil, nil, nil, nil, nil, nil)

	// Simulate concurrent access from multiple goroutines
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func() {
			counter, err := handler.GetOrCreateWriteSuccessCounter("origin", "ks1", "users")
			require.NoError(t, err)
			counter.Add(1)
			done <- true
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify total count is 100
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	for _, mf := range metricFamilies {
		if mf.GetName() == "zdm_proxy_write_success_total" {
			for _, m := range mf.GetMetric() {
				require.Equal(t, float64(100), m.GetCounter().GetValue())
			}
		}
	}
}
