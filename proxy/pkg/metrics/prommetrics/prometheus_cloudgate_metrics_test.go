package prommetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewPrometheusCloudgateProxyMetrics(t *testing.T) {
	actual := NewPrometheusMetricFactory(prometheus.NewRegistry())
	assert.NotNil(t, actual)
	assert.Empty(t, actual.registeredCollectors)
}

func TestPrometheusCloudgateProxyMetrics_AddCounter(t *testing.T) {
	registry := prometheus.NewRegistry()
	counterMetric := newTestMetric("test_counter")
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)
	counter, err := handler.GetOrCreateCounter(counterMetric)
	assert.Contains(t, handler.registeredCollectors, &collectorEntry{
		collector: counter.(*PrometheusCounter).c,
		name:      "test_counter",
	})
	assert.Nil(t, err)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	newCounter, err := handler.GetOrCreateCounter(counterMetric)
	assert.Nil(t, err)
	assert.Equal(t, counter, newCounter)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddCounterWithLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	counterMetric := newTestMetricWithLabels("test_counter", map[string]string{"counter_type": "type1"})
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)

	counter, err := handler.GetOrCreateCounter(counterMetric)
	require.Nil(t, err)
	assert.Equal(t, 1, len(handler.registeredCollectors))
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	newCounter, err := handler.GetOrCreateCounter(counterMetric)
	require.Nil(t, err)
	assert.Equal(t, counter, newCounter)

	gather, err = registry.Gather()
	assert.Len(t, gather, 1)

	counterMetric = newTestMetricWithLabels(counterMetric.GetName(), map[string]string{"counter_type": "type2"})
	newNewCounter, err := handler.GetOrCreateCounter(counterMetric)
	assert.NotEqual(t, counter, newNewCounter)
	assert.Nil(t, err)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGauge(t *testing.T) {
	registry := prometheus.NewRegistry()
	gaugeMetric := newTestMetric("test_gauge")
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)
	gauge, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	assert.Contains(t, handler.registeredCollectors, &collectorEntry{
		collector: gauge.(*PrometheusGauge).g,
		name:      "test_gauge",
	})
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	newGauge, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	assert.Equal(t, gauge, newGauge)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGaugeWithLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	gaugeMetric := newTestMetricWithLabels("test_gauge_with_labels", map[string]string{"gauge_type": "gauge1"})
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)

	g, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(handler.registeredCollectors))
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	newG, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	assert.Equal(t, g, newG)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	gaugeMetric = newTestMetricWithLabels(gaugeMetric.GetName(), map[string]string{"gauge_type": "gauge2"})
	newGWithLabel, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	assert.NotEqual(t, g, newGWithLabel)
	assert.NotEqual(t, g.(*PrometheusGauge).g, newGWithLabel.(*PrometheusGauge).g)

	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddGaugeFunction(t *testing.T) {
	registry := prometheus.NewRegistry()
	gaugeFuncMetric := newTestMetric("test_gauge_func")
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)
	gf, err := handler.GetOrCreateGaugeFunc(gaugeFuncMetric, func() float64 { return 12.34 })
	assert.Nil(t, err)
	assert.Contains(t, handler.registeredCollectors, &collectorEntry{
		collector: gf.(*PrometheusGaugeFunc).gf,
		name:      "test_gauge_func",
	})
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	newGf, err := handler.GetOrCreateGaugeFunc(gaugeFuncMetric, func() float64 { return 56.78 })
	assert.Nil(t, err)
	assert.Equal(t, gf, newGf)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddHistogram(t *testing.T) {
	registry := prometheus.NewRegistry()
	histogramMetric := newTestMetric("test_histogram")
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)
	h, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	assert.Nil(t, err)
	assert.Contains(t, handler.registeredCollectors, &collectorEntry{
		collector: h.(*PrometheusHistogram).h.(prometheus.Collector),
		name:      "test_histogram",
	})
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
	newH, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	assert.Nil(t, err)
	assert.Equal(t, h, newH)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_AddHistogramWithLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	histogramMetric := newTestMetricWithLabels("test_histogram_with_labels", map[string]string{"label1": "value1"})
	handler := NewPrometheusMetricFactory(registry)
	assert.Empty(t, handler.registeredCollectors)

	h, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(handler.registeredCollectors))
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	newH, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	require.Nil(t, err)
	assert.NotContains(t, handler.registeredCollectors, h.(*PrometheusHistogram).h)
	assert.Equal(t, 1, len(handler.registeredCollectors))
	assert.Equal(t, h, newH)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)

	histogramMetric = newTestMetricWithLabels(histogramMetric.GetName(), map[string]string{"label1": "value2"})
	newNewH, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	require.Nil(t, err)
	assert.NotContains(t, handler.registeredCollectors, h.(*PrometheusHistogram).h)
	assert.Equal(t, 1, len(handler.registeredCollectors))
	assert.NotEqual(t, h, newNewH)
	gather, err = registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 1)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Counter(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	counterMetric := newTestMetric("test_add_count_by_one_counter")
	c, err := handler.GetOrCreateCounter(counterMetric)
	assert.Nil(t, err)
	c.Add(1)
	value, err := getCounterValue(c.(*PrometheusCounter).c)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Gauge(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	gaugeMetric := newTestMetric("test_add_count_by_one_counter")
	g, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	g.Add(1)
	value, err := getGaugeValue(g.(*PrometheusGauge).g)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Counter_Labels(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	counterMetric := newTestMetricWithLabels("test_add_count_by_one_counter_labels", map[string]string{"l": "v"})
	c, err := handler.GetOrCreateCounter(counterMetric)
	assert.Nil(t, err)
	c.Add(1)
	value, err := getCounterValue(c.(*PrometheusCounter).c)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_IncrementCountByOne_Gauge_Labels(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	gaugeMetric := newTestMetricWithLabels("test_add_count_by_one_counter_labels", map[string]string{"label":"value"})
	g, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	g.Add(1)
	value, err := getGaugeValue(g.(*PrometheusGauge).g)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, value)
}

func TestPrometheusCloudgateProxyMetrics_DecrementCountByOne(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	gaugeMetric := newTestMetric("test_decrement_count_gauge")
	g, err := handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	g.Subtract(1)
	value, err := getGaugeValue(g.(*PrometheusGauge).g)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_DecrementCountByOne_Labels(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	gaugeMetric := newTestMetricWithLabels("test_decrement_count_gauge_labels", map[string]string{"label": "value"})
	g, err := handler.GetOrCreateGauge(gaugeMetric)
	g.Subtract(1)
	assert.Nil(t, err)
	value, err := getGaugeValue(g.(*PrometheusGauge).g)
	assert.Nil(t, err)
	assert.EqualValues(t, -1, value)
}

func TestPrometheusCloudgateProxyMetrics_TrackInHistogram(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	histogramMetric := newTestMetric("test_histogram")
	h, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	assert.Nil(t, err)
	begin := time.Now().Add(-time.Millisecond * 500)
	for i := 0; i < 1000; i++ {
		h.Track(begin)
	}
	count, sum, err := getHistogramValues(h.(*PrometheusHistogram).h.(prometheus.Histogram))
	assert.Nil(t, err)
	assert.EqualValues(t, 1000, count)
	// sum should contain 1000 * 0.5 = 500
	assert.InDelta(t, 500, sum, 5)
}

func TestPrometheusCloudgateProxyMetrics_TrackInHistogram_WithLabels(t *testing.T) {
	handler := NewPrometheusMetricFactory(prometheus.NewRegistry())
	histogramMetric := newTestMetricWithLabels("test_histogram_with_labels", map[string]string{"l": "v"})
	h, err := handler.GetOrCreateHistogram(histogramMetric, nil)
	assert.Nil(t, err)
	begin := time.Now().Add(-time.Millisecond * 500)
	for i := 0; i < 1000; i++ {
		h.Track(begin)
	}
	count, sum, err := getHistogramValues(h.(*PrometheusHistogram).h.(prometheus.Histogram))
	assert.Nil(t, err)
	assert.EqualValues(t, 1000, count)
	// sum should contain 1000 * 0.5 = 500
	assert.InDelta(t, 500, sum, 5)
}

func TestPrometheusCloudgateProxyMetrics_UnregisterAllMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler := NewPrometheusMetricFactory(registry)
	counterMetric := newTestMetric("test_counter")
	counterMetricWithLabels1 := newTestMetricWithLabels("test_counter_with_labels", map[string]string{"counter_type": "counter1"})
	counterMetricWithLabels2 := newTestMetricWithLabels("test_counter_with_labels", map[string]string{"counter_type": "counter2"})
	histogramMetric := newTestMetric("test_histogram")
	histogramMetricWithLabels1 := newTestMetricWithLabels("test_histogram_with_labels", map[string]string{"h": "h1"})
	histogramMetricWithLabels2 := newTestMetricWithLabels("test_histogram_with_labels", map[string]string{"h": "h2"})
	gaugeMetric := newTestMetric("test_gauge")
	gaugeFuncMetric := newTestMetric("test_gauge_func")
	_, err := handler.GetOrCreateCounter(counterMetric)
	assert.Nil(t, err)
	_, err = handler.GetOrCreateCounter(counterMetricWithLabels1)
	assert.Nil(t, err)
	_, err = handler.GetOrCreateCounter(counterMetricWithLabels2)
	assert.Nil(t, err)
	_, err = handler.GetOrCreateGauge(gaugeMetric)
	assert.Nil(t, err)
	_, err = handler.GetOrCreateGaugeFunc(gaugeFuncMetric, func() float64 { return 12.34 })
	assert.Nil(t, err)
	_, err = handler.GetOrCreateHistogram(histogramMetric, nil)
	assert.Nil(t, err)
	_, err = handler.GetOrCreateHistogram(histogramMetricWithLabels1, nil)
	assert.Nil(t, err)
	_, err = handler.GetOrCreateHistogram(histogramMetricWithLabels2, nil)
	assert.Nil(t, err)
	assert.Len(t, handler.registeredCollectors, 6)
	gather, err := registry.Gather()
	assert.Nil(t, err)
	assert.Len(t, gather, 6)

	err = handler.UnregisterAllMetrics()
	assert.Nil(t, err)
	assert.Len(t, handler.registeredCollectors, 0)
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