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
	counterMetric := newTestMetric("test_counter")
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	counter, err := handler.getCounterFromMap(counterMetric)
	assert.Nil(t, counter)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "the specified metric test_counter could not be found")
	err = handler.AddCounter(counterMetric)
	_, containsMetric := handler.collectorMap[counterMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	assert.Nil(t, err)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddCounter(counterMetric)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddCounterWithLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	counterMetric := newTestMetricWithLabels("test_counter", map[string]string{"counter_type": "type1"})
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	counter, err := handler.getCounterFromMap(counterMetric)
	assert.Nil(t, counter)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "the specified metric test_counter{counter_type=\"type1\"} could not be found")

	err = handler.AddCounter(counterMetric)
	_, containsMetric := handler.collectorMap[counterMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	assert.Nil(t, err)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	err = handler.AddCounter(counterMetric)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	counterMetric = newTestMetricWithLabels(counterMetric.GetName(), map[string]string{"counter_type": "type2"})
	err = handler.AddCounter(counterMetric)
	assert.Nil(t, err)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGauge(t *testing.T) {
	registry := prometheus.NewRegistry()
	gaugeMetric := newTestMetric("test_gauge")
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, gauge)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "the specified metric test_gauge could not be found")
	err = handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	_, containsMetric := handler.collectorMap[gaugeMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddGauge(gaugeMetric)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGaugeWithLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	gaugeMetric := newTestMetricWithLabels("test_gauge_with_labels", map[string]string{"gauge_type": "gauge1"})
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)

	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, gauge)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "the specified metric test_gauge_with_labels{gauge_type=\"gauge1\"} could not be found")

	err = handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	_, containsMetric := handler.collectorMap[gaugeMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	err = handler.AddGauge(gaugeMetric)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	gaugeMetric = newTestMetricWithLabels(gaugeMetric.GetName(), map[string]string{"gauge_type": "gauge2"})
	err = handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGaugeFunction(t *testing.T) {
	registry := prometheus.NewRegistry()
	gaugeFuncMetric := newTestMetric("test_gauge_func")
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	err := handler.AddGaugeFunction(gaugeFuncMetric, func() float64 { return 12.34 })
	assert.Nil(t, err)
	_, containsMetric := handler.collectorMap[gaugeFuncMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddGaugeFunction(gaugeFuncMetric, func() float64 { return 56.78 })
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddHistogram(t *testing.T) {
	registry := prometheus.NewRegistry()
	histogramMetric := newTestHistogram("test_histogram", nil)
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)
	err := handler.AddHistogram(histogramMetric)
	assert.Nil(t, err)
	_, containsMetric := handler.collectorMap[histogramMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	err = handler.AddHistogram(histogramMetric)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddHistogramWithLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	histogramMetric := newTestHistogramWithLabels("test_histogram_with_labels", nil, map[string]string{"label1": "value1"})
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	assert.Empty(t, handler.collectorMap)

	err := handler.AddHistogram(histogramMetric)
	assert.Nil(t, err)
	_, containsMetric := handler.collectorMap[histogramMetric.GetUniqueIdentifier()]
	assert.True(t, containsMetric)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	err = handler.AddHistogram(histogramMetric)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "duplicate metrics collector registration attempted")
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	histogramMetric = newTestHistogramWithLabels(histogramMetric.GetName(), nil, map[string]string{"label1": "value2"})
	err = handler.AddHistogram(histogramMetric)
	require.Nil(t, err)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Counter(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	counterMetric := newTestMetric("test_add_count_by_one_counter")
	err := handler.AddCounter(counterMetric)
	assert.Nil(t, err)
	err = handler.IncrementCountByOne(counterMetric)
	assert.Nil(t, err)
	counter, err := handler.getCounterFromMap(counterMetric)
	assert.Nil(t, err)
	assert.NotNil(t, counter)
	value, err := getCounterValue(counter)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Gauge(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetric("test_add_count_by_one_counter")
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.IncrementCountByOne(gaugeMetric)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Counter_Labels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	counterMetric := newTestMetricWithLabels("test_add_count_by_one_counter_labels", map[string]string{"l": "v"})
	err := handler.AddCounter(counterMetric)
	assert.Nil(t, err)
	err = handler.IncrementCountByOne(counterMetric)
	assert.Nil(t, err)
	counter, err := handler.getCounterFromMap(counterMetric)
	assert.Nil(t, err)
	assert.NotNil(t, counter)
	value, err := getCounterValue(counter)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Gauge_Labels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetricWithLabels("test_add_count_by_one_counter_labels", map[string]string{"label":"value"})
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.IncrementCountByOne(gaugeMetric)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_DecrementCountByOne(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetric("test_decrement_count_gauge")
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.DecrementCountByOne(gaugeMetric)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_DecrementCountByOne_Labels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetricWithLabels("test_decrement_count_gauge_labels", map[string]string{"label": "value"})
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.DecrementCountByOne(gaugeMetric)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_AddToCount_Counter(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	counterMetric := newTestMetric("test_add_count_counter")
	err := handler.AddCounter(counterMetric)
	assert.Nil(t, err)
	err = handler.AddToCount(counterMetric, 1)
	assert.Nil(t, err)
	counter, err := handler.getCounterFromMap(counterMetric)
	assert.Nil(t, err)
	assert.NotNil(t, counter)
	value, err := getCounterValue(counter)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_AddToCount_Gauge(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetric("test_add_count_gauge")
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.AddToCount(gaugeMetric, 1)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_SubtractFromCount(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetric("test_subtract_count_gauge")
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.SubtractFromCount(gaugeMetric, 1)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_AddToCount_Counter_Labels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	counterMetric := newTestMetricWithLabels("test_add_count_counter_labels", map[string]string{"label": "value"})
	err := handler.AddCounter(counterMetric)
	assert.Nil(t, err)
	err = handler.AddToCount(counterMetric, 1)
	assert.Nil(t, err)
	counter, err := handler.getCounterFromMap(counterMetric)
	assert.Nil(t, err)
	assert.NotNil(t, counter)
	value, err := getCounterValue(counter)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_AddToCount_Gauge_Labels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetricWithLabels("test_add_count_gauge_labels", map[string]string{"label": "value"})
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.AddToCount(gaugeMetric, 1)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_SubtractFromCount_Labels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	gaugeMetric := newTestMetricWithLabels("test_subtract_count_gauge", map[string]string{"label": "value"})
	err := handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.SubtractFromCount(gaugeMetric, 1)
	assert.Nil(t, err)
	gauge, err := handler.getGaugeFromMap(gaugeMetric)
	assert.Nil(t, err)
	assert.NotNil(t, gauge)
	value, err := getGaugeValue(gauge)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_TrackInHistogram(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	histogramMetric := newTestHistogram("test_histogram", nil)
	err := handler.AddHistogram(histogramMetric)
	assert.Nil(t, err)
	begin := time.Now().Add(-time.Millisecond * 500)
	for i := 0; i < 1000; i++ {
		err = handler.TrackInHistogram(histogramMetric, begin)
		require.Nil(t, err)
	}
	histogram, err := handler.getHistogramFromMap(histogramMetric)
	assert.Nil(t, err)
	assert.NotNil(t, histogram)
	count, sum, err := getHistogramValues(histogram)
	assert.Nil(t, err)
	assert.EqualValues(t, 1000, count)
	// sum should contain 1000 * 0.5 = 500
	assert.InDelta(t, 500, sum, 5)
}
func TestPrometheusCloudgateProxyMetrics_TrackInHistogram_WithLabels(t *testing.T) {
	handler := NewPrometheusCloudgateProxyMetrics(prometheus.NewRegistry())
	histogramMetric := newTestHistogramWithLabels("test_histogram_with_labels", nil, map[string]string{"l": "v"})
	err := handler.AddHistogram(histogramMetric)
	assert.Nil(t, err)
	begin := time.Now().Add(-time.Millisecond * 500)
	for i := 0; i < 1000; i++ {
		err = handler.TrackInHistogram(histogramMetric, begin)
		require.Nil(t, err)
	}
	histogram, err := handler.getHistogramFromMap(histogramMetric)
	assert.Nil(t, err)
	assert.NotNil(t, histogram)
	count, sum, err := getHistogramValues(histogram)
	assert.Nil(t, err)
	assert.EqualValues(t, 1000, count)
	// sum should contain 1000 * 0.5 = 500
	assert.InDelta(t, 500, sum, 5)
}

func TestPrometheusCloudgateProxyMetrics_UnregisterAllMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusCloudgateProxyMetrics(registry)
	counterMetric := newTestMetric("test_counter")
	counterMetricWithLabels1 := newTestMetricWithLabels("test_counter_with_labels", map[string]string{"counter_type": "counter1"})
	counterMetricWithLabels2 := newTestMetricWithLabels("test_counter_with_labels", map[string]string{"counter_type": "counter2"})
	histogramMetric := newTestHistogram("test_histogram", nil)
	histogramMetricWithLabels1 := newTestHistogramWithLabels("test_histogram_with_labels", nil, map[string]string{"h": "h1"})
	histogramMetricWithLabels2 := newTestHistogramWithLabels("test_histogram_with_labels", nil, map[string]string{"h": "h2"})
	gaugeMetric := newTestMetric("test_gauge")
	gaugeFuncMetric := newTestMetric("test_gauge_func")
	err := handler.AddCounter(counterMetric)
	assert.Nil(t, err)
	err = handler.AddCounter(counterMetricWithLabels1)
	assert.Nil(t, err)
	err = handler.AddCounter(counterMetricWithLabels2)
	assert.Nil(t, err)
	err = handler.AddGauge(gaugeMetric)
	assert.Nil(t, err)
	err = handler.AddGaugeFunction(gaugeFuncMetric, func() float64 { return 12.34 })
	assert.Nil(t, err)
	err = handler.AddHistogram(histogramMetric)
	assert.Nil(t, err)
	err = handler.AddHistogram(histogramMetricWithLabels1)
	assert.Nil(t, err)
	err = handler.AddHistogram(histogramMetricWithLabels2)
	assert.Nil(t, err)
	assert.Len(t, handler.collectorMap, 8)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 6)

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

func newTestMetric(name string) metrics.Metric {
	return metrics.NewMetric(name, "")
}

func newTestMetricWithLabels(name string, labels map[string]string) metrics.Metric {
	return metrics.NewMetricWithLabels(name, "", labels)
}

func newTestHistogram(name string, buckets []float64) metrics.HistogramMetric {
	return metrics.NewHistogramMetric(name, "", buckets)
}

func newTestHistogramWithLabels(name string, buckets []float64, labels map[string]string) metrics.HistogramMetric {
	return metrics.NewHistogramMetricWithLabels(name, "", buckets, labels)
}