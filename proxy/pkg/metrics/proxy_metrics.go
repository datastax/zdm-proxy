package metrics

const (
	destinationOrigin = "origin"
	destinationTarget = "target"
	destinationBoth = "both"

	failedRequestsName = "proxy_requests_failed_total"
	failedRequestsSentFailedLabel = "sent_failed"
	sentOriginFailedOrigin = "sent_origin_failed_origin"
	sentTargetFailedTarget = "sent_target_failed_target"
	sentBothFailedOrigin = "sent_both_failed_origin"
	sentBothFailedTarget = "sent_both_failed_target"
	sentBothFailedBoth = "sent_both_failed_both"

	failedRequestsDescription = "Running total of failed requests"

	requestDurationName = "proxy_request_duration_seconds"
	requestDurationDestinationLabel = "destination"
	requestDurationDescription = "Histogram that tracks the latency of requests at proxy entry point"

	inFlightRequestsName = "proxy_inflight_requests_total"
	inFlightRequestsDestinationLabel = "destination"
	inFlightRequestsDescription = "Number of requests currently in flight in the proxy"
)

var (
	FailedRequestsOrigin = NewMetricWithLabels(
		failedRequestsName,
		failedRequestsDescription,
		map[string]string{
			failedRequestsSentFailedLabel: sentOriginFailedOrigin,
		},
	)
	FailedRequestsTarget = NewMetricWithLabels(
		failedRequestsName,
		failedRequestsDescription,
		map[string]string{
			failedRequestsSentFailedLabel: sentTargetFailedTarget,
		},
	)
	FailedRequestsBothFailedOnOriginOnly = NewMetricWithLabels(
		failedRequestsName,
		failedRequestsDescription,
		map[string]string{
			failedRequestsSentFailedLabel: sentBothFailedOrigin,
		},
	)
	FailedRequestsBothFailedOnTargetOnly = NewMetricWithLabels(
		failedRequestsName,
		failedRequestsDescription,
		map[string]string{
			failedRequestsSentFailedLabel: sentBothFailedTarget,
		},
	)
	FailedRequestsBoth = NewMetricWithLabels(
		failedRequestsName,
		failedRequestsDescription,
		map[string]string{
			failedRequestsSentFailedLabel: sentBothFailedBoth,
		},
	)

	PSCacheSize = NewMetric(
		"pscache_entries_total",
		"Number of entries currently in the prepared statement cache",
	)
	PSCacheMissCount = NewMetric(
		"pscache_miss_total",
		"Running total of prepared statement cache misses in the proxy",
	)

	ProxyRequestDurationOrigin = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			requestDurationDestinationLabel: destinationOrigin,
		},
	)
	ProxyRequestDurationTarget = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			requestDurationDestinationLabel: destinationTarget,
		},
	)
	ProxyRequestDurationBoth = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			requestDurationDestinationLabel: destinationBoth,
		},
	)

	InFlightRequestsOrigin = NewMetricWithLabels(
		inFlightRequestsName,
		inFlightRequestsDescription,
		map[string]string{
			inFlightRequestsDestinationLabel: destinationOrigin,
		},
	)
	InFlightRequestsTarget = NewMetricWithLabels(
		inFlightRequestsName,
		inFlightRequestsDescription,
		map[string]string{
			inFlightRequestsDestinationLabel: destinationTarget,
		},
	)
	InFlightRequestsBoth = NewMetricWithLabels(
		inFlightRequestsName,
		inFlightRequestsDescription,
		map[string]string{
			inFlightRequestsDestinationLabel: destinationBoth,
		},
	)

	OpenClientConnections = NewMetric(
		"client_connections_total",
		"Number of client connections currently open",
	)
)
