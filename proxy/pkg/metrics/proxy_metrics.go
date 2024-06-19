package metrics

const (
	typeReadsOrigin = "reads_origin"
	typeReadsTarget = "reads_target"
	TypeWrites      = "writes"
	TypeReads       = "reads"
	TypeOther       = "other"

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
	RequestDurationTypeLabel   = "type"
	requestDurationDescription = "Histogram that tracks the latency of requests at proxy entry point"

	inFlightRequestsName        = "proxy_inflight_requests_total"
	inFlightRequestsTypeLabel   = "type"
	inFlightRequestsDescription = "Number of requests currently in flight in the proxy"
)

var (
	StatementCategories = []string{TypeWrites, TypeReads, TypeOther}
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
			RequestDurationTypeLabel: typeReadsOrigin,
		},
	)
	ProxyReadsTargetDuration = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			RequestDurationTypeLabel: typeReadsTarget,
		},
	)
	ProxyWritesDuration = NewMetricWithLabels(
		requestDurationName,
		requestDurationDescription,
		map[string]string{
			RequestDurationTypeLabel: TypeWrites,
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
			inFlightRequestsTypeLabel: TypeWrites,
		},
	)

	OpenClientConnections = NewMetric(
		"client_connections_total",
		"Number of client connections currently open",
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
}
