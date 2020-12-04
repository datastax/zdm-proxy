package metrics

import (
	"net/http"
	"time"
)

type IMetricsHandler interface {
	AddCounter(mn Metric) error
	AddGauge(mn Metric) error
	AddGaugeFunction(mn Metric, mf func() float64) error
	AddHistogram(mn Metric, buckets []float64) error

	// Increments an existing metric by one. The metric must be a counter or a gauge.
	// An error is returned if the metric does not exist, or is not a counter nor a gauge.
	IncrementCountByOne(mn Metric) error

	// Decrements an existing metric by one. The metric must be a gauge.
	// An error is returned if the metric does not exist, or is not a gauge.
	DecrementCountByOne(mn Metric) error

	// Adds the given value to an existing metric. The metric can be either a counter or a gauge.
	// An error is returned if the metric does not exist, or is not a counter nor a gauge; an error is
	// also returned if the metric is a counter, and valueToAdd is negative.
	AddToCount(mn Metric, valueToAdd int) error

	// Subtracts the given value to an existing metric. The metric must be a gauge.
	// An error is returned if the metric does not exist, or is not a gauge.
	SubtractFromCount(mn Metric, valueToSubtract int) error

	// Tracks startTime, or more precisely, the duration obtained from time.Since(startTime).
	// An error is returned if the metric does not exist, or is not a histogram.
	TrackInHistogram(mn Metric, startTime time.Time) error

	// Unregisters all registered metrics and discards all internal references to them.
	// An error is returned if at least one metric could not be unregistered.
	UnregisterAllMetrics() error

	// Returns the http handler implementation for the metrics endpoint.
	Handler() http.Handler
}

func DefaultHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "Proxy metrics haven't been initialized yet.", http.StatusServiceUnavailable)
	})
}