package noopmetrics

import (
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"net/http"
	"time"
)

type noopMetricsHandler struct {}

func NewNoopMetricsHandler() metrics.IMetricsHandler {
	return &noopMetricsHandler{}
}

func (noop *noopMetricsHandler) AddCounter(mn metrics.Metric) error {
	return nil
}

func (noop *noopMetricsHandler) AddGauge(mn metrics.Metric) error {
	return nil
}

func (noop *noopMetricsHandler) AddGaugeFunction(mn metrics.Metric, mf func() float64) error {
	return nil
}

func (noop *noopMetricsHandler) AddHistogram(mn metrics.Metric, buckets []float64) error {
	return nil
}

func (noop *noopMetricsHandler) IncrementCountByOne(mn metrics.Metric) error {
	return nil
}

func (noop *noopMetricsHandler) DecrementCountByOne(mn metrics.Metric) error {
	return nil
}

func (noop *noopMetricsHandler) AddToCount(mn metrics.Metric, valueToAdd int) error {
	return nil
}

func (noop *noopMetricsHandler) SubtractFromCount(mn metrics.Metric, valueToSubtract int) error {
	return nil
}

func (noop *noopMetricsHandler) TrackInHistogram(mn metrics.Metric, startTime time.Time) error {
	return nil
}

func (noop *noopMetricsHandler) UnregisterAllMetrics() error {
	return nil
}

// Returns the http handler implementation for the metrics endpoint.
func (noop *noopMetricsHandler) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "Metrics are disabled on this proxy instance.", http.StatusNotFound)
	})
}