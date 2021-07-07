package metrics

import (
	"net/http"
)

type MetricFactory interface {
	GetOrCreateCounter(mn Metric) (Counter, error)
	GetOrCreateGauge(mn Metric) (Gauge, error)
	GetOrCreateGaugeFunc(mn Metric, mf func() float64) (GaugeFunc, error)
	GetOrCreateHistogram(mn Metric, buckets []float64) (Histogram, error)

	// Unregisters all registered metrics and discards all internal references to them.
	// An error is returned if at least one metric could not be unregistered.
	UnregisterAllMetrics() error

	// Returns the http handler implementation for the metrics endpoint.
	HttpHandler() http.Handler
}

func DefaultHttpHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "Proxy metrics haven't been initialized yet.", http.StatusServiceUnavailable)
	})
}