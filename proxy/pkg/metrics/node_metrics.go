package metrics

const (
	originFailedRequestsName        = "origin_requests_failed_total"
	originFailedRequestsErrorLabel  = "error"
	originFailedRequestsDescription = "Running total of requests that failed on Origin Cluster"

	targetFailedRequestsName        = "target_requests_failed_total"
	targetFailedRequestsErrorLabel  = "error"
	targetFailedRequestsDescription = "Running total of requests that failed on Target Cluster"

	asyncFailedRequestsName        = "async_requests_failed_total"
	asyncFailedRequestsErrorLabel  = "error"
	asyncFailedRequestsDescription = "Running total of requests that failed on Async Connector"

	errorClientTimeout = "client_timeout"
	errorReadTimeout   = "read_timeout"
	errorReadFailure   = "read_failure"
	errorWriteTimeout  = "write_timeout"
	errorWriteFailure  = "write_failure"
	errorOverloaded    = "overloaded"
	errorUnavailable   = "unavailable"
	errorUnprepared    = "unprepared"
	errorOther         = "other"

	nodeLabel = "node"
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
	OriginReadFailures = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorReadFailure,
		},
	)
	OriginWriteTimeouts = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorWriteTimeout,
		},
	)
	OriginWriteFailures = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorWriteFailure,
		},
	)
	OriginUnpreparedErrors = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorUnprepared,
		},
	)
	OriginOverloadedErrors = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorOverloaded,
		},
	)
	OriginUnavailableErrors = NewMetricWithLabels(
		originFailedRequestsName,
		originFailedRequestsDescription,
		map[string]string{
			originFailedRequestsErrorLabel: errorUnavailable,
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
	TargetReadFailures = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorReadFailure,
		},
	)
	TargetWriteTimeouts = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorWriteTimeout,
		},
	)
	TargetWriteFailures = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorWriteFailure,
		},
	)
	TargetUnpreparedErrors = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorUnprepared,
		},
	)
	TargetOverloadedErrors = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorOverloaded,
		},
	)
	TargetUnavailableErrors = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorUnavailable,
		},
	)
	TargetOtherErrors = NewMetricWithLabels(
		targetFailedRequestsName,
		targetFailedRequestsDescription,
		map[string]string{
			targetFailedRequestsErrorLabel: errorOther,
		},
	)

	AsyncClientTimeouts = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorClientTimeout,
		},
	)
	AsyncReadTimeouts = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorReadTimeout,
		},
	)
	AsyncReadFailures = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorReadFailure,
		},
	)
	AsyncWriteTimeouts = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorWriteTimeout,
		},
	)
	AsyncWriteFailures = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorWriteFailure,
		},
	)
	AsyncUnpreparedErrors = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorUnprepared,
		},
	)
	AsyncOverloadedErrors = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorOverloaded,
		},
	)
	AsyncUnavailableErrors = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorUnavailable,
		},
	)
	AsyncOtherErrors = NewMetricWithLabels(
		asyncFailedRequestsName,
		asyncFailedRequestsDescription,
		map[string]string{
			asyncFailedRequestsErrorLabel: errorOther,
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
	AsyncRequestDuration = NewMetric(
		"async_request_duration_seconds",
		"Histogram that tracks the latency of async requests at async cluster connector level",
	)

	OpenOriginConnections = NewMetric(
		"origin_connections_total",
		"Number of connections to Origin Cassandra currently open",
	)
	OpenTargetConnections = NewMetric(
		"target_connections_total",
		"Number of connections to Target Cassandra currently open",
	)
	OpenAsyncConnections = NewMetric(
		"async_connections_total",
		"Number of connections currently open for async requests",
	)

	InFlightRequestsAsync = NewMetric(
		"async_inflight_requests_total",
		"Number of async requests currently in flight",
	)

	OriginUsedStreamIds = NewMetric(
		"origin_used_stream_ids_total",
		"Number of used stream ids in Origin connections")

	TargetUsedStreamIds = NewMetric(
		"target_used_stream_ids_total",
		"Number of used stream ids in Target connections")

	AsyncUsedStreamIds = NewMetric(
		"async_used_stream_ids_total",
		"Number of used stream ids in Async connections")
)

type NodeMetrics struct {
	OriginMetrics *NodeMetricsInstance
	TargetMetrics *NodeMetricsInstance
	AsyncMetrics  *NodeMetricsInstance
}

type NodeMetricsInstance struct {
	ClientTimeouts    Counter
	ReadTimeouts      Counter
	ReadFailures      Counter
	WriteTimeouts     Counter
	WriteFailures     Counter
	UnpreparedErrors  Counter
	OverloadedErrors  Counter
	UnavailableErrors Counter
	OtherErrors       Counter

	ReadDurations  Histogram
	WriteDurations Histogram

	OpenConnections Gauge

	InFlightRequests Gauge

	UsedStreamIds Gauge
}

func CreateCounterNodeMetric(metricFactory MetricFactory, nodeDescription string, mn Metric) (Counter, error) {
	m, err := metricFactory.GetOrCreateCounter(
		mn.WithLabels(map[string]string{nodeLabel: nodeDescription}))
	if err != nil {
		return nil, err
	}
	return m, nil
}

func CreateHistogramNodeMetric(metricFactory MetricFactory, nodeDescription string, mn Metric, buckets []float64, labels ...string) (Histogram, error) {
	customLabels := make(map[string]string)
	for i := 0; i < len(labels); i = i + 2 {
		customLabels[labels[i]] = labels[i+1]
	}
	m, err := metricFactory.GetOrCreateHistogram(
		mn.WithLabels(map[string]string{nodeLabel: nodeDescription}).WithLabels(customLabels), buckets)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func CreateGaugeNodeMetric(metricFactory MetricFactory, nodeDescription string, mn Metric) (Gauge, error) {
	m, err := metricFactory.GetOrCreateGauge(
		mn.WithLabels(map[string]string{nodeLabel: nodeDescription}))
	if err != nil {
		return nil, err
	}
	return m, nil
}
