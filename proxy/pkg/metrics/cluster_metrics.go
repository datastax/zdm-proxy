package metrics

const (
	originFailedRequestsName = "origin_requests_failed_total"
	originFailedRequestsErrorLabel = "error"
	originFailedRequestsDescription = "Running total of requests that failed on Origin Cassandra"

	targetFailedRequestsName = "target_requests_failed_total"
	targetFailedRequestsErrorLabel = "error"
	targetFailedRequestsDescription = "Running total of requests that failed on Target Cassandra"

	errorClientTimeout = "client_timeout"
	errorReadTimeout = "read_timeout"
	errorWriteTimeout = "write_timeout"
	errorUnprepared = "unprepared"
	errorOther = "other"
)

var (
	OriginClientTimeouts = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorClientTimeout,
		},
	)
	OriginReadTimeouts = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorReadTimeout,
		},
	)
	OriginWriteTimeouts = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorWriteTimeout,
		},
	)
	OriginUnpreparedErrors = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorUnprepared,
		},
	)
	OriginOtherErrors = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorOther,
		},
	)

	TargetClientTimeouts = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorClientTimeout,
		},
	)
	TargetReadTimeouts = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorReadTimeout,
		},
	)
	TargetWriteTimeouts = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorWriteTimeout,
		},
	)
	TargetUnpreparedErrors = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorUnprepared,
		},
	)
	TargetOtherErrors = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorOther,
		},
	)

	OriginRequestDuration = NewMetric(
		"origin_request_duration_seconds",
		"Histogram that tracks the latency of requests sent to origin clusters at cluster connector level",
	)
	TargetRequestDuration = NewMetric(
		"target_request_duration_seconds",
		"Histogram that tracks the latency of requests sent to target clusters at cluster connector level",
	)

	OpenOriginConnections = NewMetric(
		"origin_connections_total",
		"Number of connections to Origin Cassandra currently open",
	)
	OpenTargetConnections = NewMetric(
		"target_connections_total",
		"Number of connections to Target Cassandra currently open",
	)
)