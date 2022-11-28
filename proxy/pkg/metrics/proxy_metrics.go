package metrics

const (
	typeReadsOrigin = "reads_origin"
	typeReadsTarget = "reads_target"
	typeWrites      = "writes"
	typeCount       = "count"

	failedRequestsClusterOrigin = "origin"
	failedRequestsClusterTarget = "target"
	failedRequestsClusterBoth   = "both"

	failedReadsName         = "proxy_failed_reads_total"
	failedReadsDescription  = "Running total of failed reads"
	failedReadsClusterLabel = "cluster"

	failedWritesName                     = "proxy_failed_writes_total"
	failedWritesDescription              = "Running total of failed writes"
	failedWritesFailedOnClusterTypeLabel = "failed_on"

	requestDurationName        = "proxy_request_duration_seconds"
	requestDurationTypeLabel   = "type"
	requestDurationDescription = "Histogram that tracks the latency of requests at proxy entry point"

	inFlightRequestsName        = "proxy_inflight_requests_total"
	inFlightRequestsTypeLabel   = "type"
	inFlightRequestsDescription = "Number of requests currently in flight in the proxy"

	usedStreamIdsOriginName        = "proxy_used_stream_ids_origin"
	usedStreamIdsOriginDescription = "Number of used stream ids for ORIGIN connector"

	usedStreamIdsTargetName        = "proxy_used_stream_ids_target"
	usedStreamIdsTargetDescription = "Number of used stream ids for TARGET connector"

	usedStreamIdsAsyncName        = "proxy_used_stream_ids_async"
	usedStreamIdsAsyncDescription = "Number of used stream ids for ASYNC connector"

	usedStreamIdsControlName        = "proxy_used_stream_ids_control"
	usedStreamIdsControlDescription = "Number of used stream ids for CONTROL connector"

	usedStreamIdsTypeLabel = "type"
)

var (
	FailedReadsOrigin = NewMetricWithLabels(
		failedReadsName,
		failedReadsDescription,
		map[string]string{
			failedReadsClusterLabel: failedRequestsClusterOrigin,
		},
	)
	FailedReadsTarget = NewMetricWithLabels(
		failedReadsName,
		failedReadsDescription,
		map[string]string{
			failedReadsClusterLabel: failedRequestsClusterTarget,
		},
	)
	FailedWritesOnOrigin = NewMetricWithLabels(
		failedWritesName,
		failedWritesDescription,
		map[string]string{
			failedWritesFailedOnClusterTypeLabel: failedRequestsClusterOrigin,
		},
	)
	FailedWritesOnTarget = NewMetricWithLabels(
		failedWritesName,
		failedWritesDescription,
		map[string]string{
			failedWritesFailedOnClusterTypeLabel: failedRequestsClusterTarget,
		},
	)
	FailedWritesOnBoth = NewMetricWithLabels(
		failedWritesName,
		failedWritesDescription,
		map[string]string{
			failedWritesFailedOnClusterTypeLabel: failedRequestsClusterBoth,
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

	ProxyReadsOriginDuration = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			requestDurationTypeLabel: typeReadsOrigin,
		},
	)
	ProxyReadsTargetDuration = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			requestDurationTypeLabel: typeReadsTarget,
		},
	)
	ProxyWritesDuration = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			requestDurationTypeLabel: typeWrites,
		},
	)

	InFlightReadsOrigin = NewMetricWithLabels(
		inFlightRequestsName,
		inFlightRequestsDescription,
		map[string]string{
			inFlightRequestsTypeLabel: typeReadsOrigin,
		},
	)
	InFlightReadsTarget = NewMetricWithLabels(
		inFlightRequestsName,
		inFlightRequestsDescription,
		map[string]string{
			inFlightRequestsTypeLabel: typeReadsTarget,
		},
	)
	InFlightWrites = NewMetricWithLabels(
		inFlightRequestsName,
		inFlightRequestsDescription,
		map[string]string{
			inFlightRequestsTypeLabel: typeWrites,
		},
	)

	OpenClientConnections = NewMetric(
		"client_connections_total",
		"Number of client connections currently open",
	)

	UsedStreamIdsOrigin = NewMetricWithLabels(
		usedStreamIdsOriginName,
		usedStreamIdsOriginDescription,
		map[string]string{
			usedStreamIdsTypeLabel: typeCount,
		},
	)
	UsedStreamIdsTarget = NewMetricWithLabels(
		usedStreamIdsTargetName,
		usedStreamIdsTargetDescription,
		map[string]string{
			usedStreamIdsTypeLabel: typeCount,
		},
	)
	UsedStreamIdsAsync = NewMetricWithLabels(
		usedStreamIdsAsyncName,
		usedStreamIdsAsyncDescription,
		map[string]string{
			usedStreamIdsTypeLabel: typeCount,
		},
	)
	UsedStreamIdsControl = NewMetricWithLabels(
		usedStreamIdsControlName,
		usedStreamIdsControlDescription,
		map[string]string{
			usedStreamIdsTypeLabel: typeCount,
		},
	)
)

type ProxyMetrics struct {
	FailedReadsOrigin    Counter
	FailedReadsTarget    Counter
	FailedWritesOnOrigin Counter
	FailedWritesOnTarget Counter
	FailedWritesOnBoth   Counter

	PSCacheSize      GaugeFunc
	PSCacheMissCount Counter

	ProxyReadsOriginDuration Histogram
	ProxyReadsTargetDuration Histogram
	ProxyWritesDuration      Histogram

	InFlightReadsOrigin Gauge
	InFlightReadsTarget Gauge
	InFlightWrites      Gauge

	OpenClientConnections GaugeFunc

	ProxyUsedStreamIdsOrigin  Gauge
	ProxyUsedStreamIdsTarget  Gauge
	ProxyUsedStreamIdsAsync   Gauge
	ProxyUsedStreamIdsControl Gauge
}
