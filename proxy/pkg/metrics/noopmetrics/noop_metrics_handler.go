package noopmetrics

import (
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"net/http"
	"time"
)

type noopMetricFactory struct{}

func NewNoopMetricFactory() metrics.MetricFactory {
	return &noopMetricFactory{}
}

func (noop *noopMetricFactory) GetOrCreateCounter(mn metrics.Metric) (metrics.Counter, error) {
	return &NoopMetric{}, nil
}

func (noop *noopMetricFactory) GetOrCreateGauge(mn metrics.Metric) (metrics.Gauge, error) {
	return &NoopMetric{}, nil
}

func (noop *noopMetricFactory) GetOrCreateGaugeFunc(mn metrics.Metric, mf func() float64) (metrics.GaugeFunc, error) {
	return &NoopMetric{}, nil
}

func (noop *noopMetricFactory) GetOrCreateHistogram(mn metrics.Metric, buckets []float64) (metrics.Histogram, error) {
	return &NoopMetric{}, nil
}

func (noop *noopMetricFactory) UnregisterAllMetrics() error {
	return nil
}

// HttpHandler returns the http handler implementation for the metrics endpoint.
func (noop *noopMetricFactory) HttpHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "Metrics are disabled on this proxy instance.", http.StatusNotFound)
	})
}

type NoopMetric struct{}

func (recv *NoopMetric) Add(val int) {}

func (recv *NoopMetric) Subtract(val int) {}

func (recv *NoopMetric) Track(begin time.Time) {}
