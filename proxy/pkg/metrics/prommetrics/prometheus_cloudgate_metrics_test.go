package prommetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewPrometheusCloudgateProxyMetrics(t *testing.T) {
	actual := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	assert.NotNil(t, actual)
	assert.Empty(t, actual.collectorMap)
}

func TestPrometheusCloudgateProxyMetrics_AddCounter(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	counter, err := handler.getCounterFromMap("test_counter")
	assert.Nil(t, counter)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "the specified metrics could not be found")
	err = handler.AddCounter("test_counter")
	assert.Contains(t, handler.collectorMap, metrics.MetricName("test_counter"))
	assert.Nil(t, err)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddCounter("test_counter")
	assert.NotNil(t, err)
	assert.EqualError(t, err, "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGauge(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	gauge, err := handler.getGaugeFromMap("test_gauge")
	assert.Nil(t, gauge)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "the specified metrics could not be found")
	err = handler.AddGauge("test_gauge")
	assert.Nil(t, err)
	assert.Contains(t, handler.collectorMap, metrics.MetricName("test_gauge"))
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddGauge("test_gauge")
	assert.NotNil(t, err)
	assert.EqualError(t, err, "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGaugeFunction(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	err := handler.AddGaugeFunction("test_gauge_func", func() float64 { return 12.34 })
	assert.Nil(t, err)
	assert.Contains(t, handler.collectorMap, metrics.MetricName("test_gauge_func"))
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddGaugeFunction("test_gauge_func", func() float64 { return 56.78 })
	assert.NotNil(t, err)
	assert.EqualError(t, err, "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddHistogram(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	err := handler.AddHistogram("test_histogram")
	assert.Nil(t, err)
	assert.Contains(t, handler.collectorMap, metrics.MetricName("test_histogram"))
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddHistogram("test_histogram")
	assert.NotNil(t, err)
	assert.EqualError(t, err, "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Counter(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddCounter("test_add_count_by_one_counter")
	assert.Nil(t, err)
	err = handler.IncrementCountByOne("test_add_count_by_one_counter")
	assert.Nil(t, err)
	counter, err := handler.getCounterFromMap("test_add_count_by_one_counter")
	assert.Nil(t, err)
	assert.NotNil(t, counter)
	value, err := getCounterValue(counter)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Gauge(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddGauge("test_add_count_by_one_gauge")
	assert.Nil(t, err)
	err = handler.IncrementCountByOne("test_add_count_by_one_gauge")
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap("test_add_count_by_one_gauge")
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_DecrementCountByOne(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddGauge("test_decrement_count_gauge")
	assert.Nil(t, err)
	err = handler.DecrementCountByOne("test_decrement_count_gauge")
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap("test_decrement_count_gauge")
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)

}

func TestPrometheusCloudgateProxyMetrics_AddToCount_Counter(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddCounter("test_add_count_counter")
	assert.Nil(t, err)
	err = handler.AddToCount("test_add_count_counter", 1)
	assert.Nil(t, err)
	counter, err := handler.getCounterFromMap("test_add_count_counter")
	assert.Nil(t, err)
	assert.NotNil(t, counter)
	value, err := getCounterValue(counter)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_AddToCount_Gauge(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddGauge("test_add_count_gauge")
	assert.Nil(t, err)
	err = handler.AddToCount("test_add_count_gauge", 1)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap("test_add_count_gauge")
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_SubtractFromCount(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddGauge("test_subtract_count_gauge")
	assert.Nil(t, err)
	err = handler.SubtractFromCount("test_subtract_count_gauge", 1)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap("test_subtract_count_gauge")
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_TrackInHistogram(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	err := handler.AddHistogram("test_histogram")
	assert.Nil(t, err)
	begin := time.Now().Add(-time.Millisecond * 500)
	for i := 0; i < 1000; i++ {
		err = handler.TrackInHistogram("test_histogram", begin)
		require.Nil(t, err)
	}
	histogram, err := handler.getHistogramFromMap("test_histogram")
	assert.Nil(t, err)
	assert.NotNil(t, histogram)
	count, sum, err := getHistogramValues(histogram)
	assert.Nil(t, err)
	assert.EqualValues(t, 1000, count)
	// sum should contain 1000 * 0.5 = 500
	assert.InDelta(t, 500, sum, 1)
}

func TestPrometheusCloudgateProxyMetrics_UnregisterAllMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	err := handler.AddCounter("test_counter")
	assert.Nil(t, err)
	err = handler.AddGauge("test_gauge")
	assert.Nil(t, err)
	err = handler.AddGaugeFunction("test_gauge_func", func() float64 { return 12.34 })
	assert.Nil(t, err)
	err = handler.AddHistogram("test_histogram")
	assert.Nil(t, err)
	assert.Len(t, handler.collectorMap, 4)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 4)
	err = handler.UnregisterAllMetrics()
	assert.Nil(t, err)
	assert.Len(t, handler.collectorMap, 0)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 0)
}

func getCounterValue(counter prometheus.Counter) (float64, error) {
	var m = &dto.Metric{}
	if err := counter.Write(m); err != nil {
		return 0, err
	}
	return m.Counter.GetValue(), nil
}

func getGaugeValue(gauge prometheus.Gauge) (float64, error) {
	var m = &dto.Metric{}
	if err := gauge.Write(m); err != nil {
		return 0, err
	}
	return m.Gauge.GetValue(), nil
}

func getHistogramValues(histogram prometheus.Histogram) (uint64, float64, error) {
	var m = &dto.Metric{}
	if err := histogram.Write(m); err != nil {
		return 0, 0, err
	}
	return m.Histogram.GetSampleCount(), m.Histogram.GetSampleSum(), nil
}
