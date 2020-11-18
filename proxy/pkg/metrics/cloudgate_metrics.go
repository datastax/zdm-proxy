package metrics

import (
	"net/http"
	"time"
)

/*
	Counters:

	[These four could become histograms?]
	 - Read requests, successful
	 - Read requests, failed
	 - Write requests, successful
	 - Write requests, failed (both / Origin only / Target only)

	 - Prepared Statement Cache miss
	 - Unprepared statements on Origin Cassandra
	 - Unprepared statements on Target Cassandra

	Histograms:
	 - Read requests, latency
	 - Write requests, latency

	Gauges:
	 - Total number of requests currently in flight
	 - Total number of goroutines currently running
	 - Total number of client connections currently in place
	 - Total number of connections to Origin Cassandra currently in place
	 - Total number of connections to Target Cassandra currently in place


*/
type MetricName string

const (
	SuccessReads = MetricName("SuccessfulReadRequests")
	FailedReads  = MetricName("FailedReadRequests")

	SuccessBothWrites      = MetricName("SuccessfulOnBothWriteRequests")
	FailedOriginOnlyWrites = MetricName("FailedOnOriginOnlyWriteRequests")
	FailedTargetOnlyWrites = MetricName("FailedOnTargetOnlyWriteRequests")
	FailedBothWrites       = MetricName("FailedOnBothWriteRequests")

	TimeOutsProxyOrigin        = MetricName("RequestsTimedOutOnProxyFromOrigin")
	TimeOutsProxyTarget        = MetricName("RequestsTimedOutOnProxyFromTarget")
	ReadTimeOutsOriginCluster  = MetricName("ReadsTimedOutOnOriginCluster")
	WriteTimeOutsOriginCluster = MetricName("WritesTimedOutOnOriginCluster")
	WriteTimeOutsTargetCluster = MetricName("WritesTimedOutOnTargetCluster")

	UnpreparedReads        = MetricName("UnpreparedReadRequestCount")
	UnpreparedOriginWrites = MetricName("UnpreparedWriteRequestOnOriginCount")
	UnpreparedTargetWrites = MetricName("UnpreparedWriteRequestOnTargetCount")
	PSCacheSize            = MetricName("PreparedStatementCacheNumberOfEntries")
	PSCacheMissCount       = MetricName("PreparedStatementCacheMissCount")

	ProxyReadLatencyHist   = MetricName("ReadRequestProxyLatencyHist")
	OriginReadLatencyHist  = MetricName("ReadRequestOriginLatencyHist")
	ProxyWriteLatencyHist  = MetricName("ProxyWriteRequestLatencyHist")
	OriginWriteLatencyHist = MetricName("OriginWriteRequestLatencyHist")
	TargetWriteLatencyHist = MetricName("TargetWriteRequestLatencyHist")

	InFlightReadRequests  = MetricName("InFlightReadRequests")
	InFlightWriteRequests = MetricName("InFlightWriteRequests")
	OpenClientConnections = MetricName("OpenClientConnections")
	OpenOriginConnections = MetricName("OpenOriginConnections")
	OpenTargetConnections = MetricName("OpenTargetConnections")
)

func (mn MetricName) GetDescription() string {
	switch mn {
	case SuccessReads:
		return "Running total of successful read requests"
	case FailedReads:
		return "Running total of failed read requests"
	case SuccessBothWrites:
		return "Running total of successful write requests"
	case FailedOriginOnlyWrites:
		return "Running total of write requests that failed on Origin Cassandra only"
	case FailedTargetOnlyWrites:
		return "Running total of write requests that failed on Target Cassandra only"
	case FailedBothWrites:
		return "Running total of write requests that failed on both clusters"
	case TimeOutsProxyOrigin:
		return "Running total of requests that timed out at proxy level from Origin Cassandra"
	case TimeOutsProxyTarget:
		return "Running total of requests that timed out at proxy level from Target Cassandra"
	case ReadTimeOutsOriginCluster:
		return "Running total of read requests that timed out at cluster level on Origin Cassandra"
	case WriteTimeOutsOriginCluster:
		return "Running total of write requests that timed out at cluster level on Origin Cassandra"
	case WriteTimeOutsTargetCluster:
		return "Running total of write requests that timed out at cluster level on Target Cassandra"
	case PSCacheSize:
		return "Number of entries currently in the prepared statement cache"
	case PSCacheMissCount:
		return "Running total of prepared statement cache misses in the proxy"
	case UnpreparedReads:
		return "Running total of PreparedStatement reads that were found to be unprepared on Origin Cassandra"
	case UnpreparedOriginWrites:
		return "Running total of PreparedStatement writes that were found to be unprepared on Origin Cassandra"
	case UnpreparedTargetWrites:
		return "Running total of PreparedStatement writes that were found to be unprepared on Target Cassandra"
	case ProxyReadLatencyHist:
		return "Histogram for read request latency at proxy entry point"
	case OriginReadLatencyHist:
		return "Histogram for read request latency on Origin Cassandra"
	case ProxyWriteLatencyHist:
		return "Histogram for write request latency at proxy entry point"
	case OriginWriteLatencyHist:
		return "Histogram for write request latency on Origin Cassandra"
	case TargetWriteLatencyHist:
		return "Histogram for write request latency on Target Cassandra"
	case InFlightReadRequests:
		return "Number of client read requests currently in flight in the proxy"
	case InFlightWriteRequests:
		return "Number of client write requests currently in flight in the proxy"
	case OpenClientConnections:
		return "Number of client connections currently open"
	case OpenOriginConnections:
		return "Number of connections to Origin Cassandra currently open"
	case OpenTargetConnections:
		return "Number of connections to Target Cassandra currently open"
	default:
		return "Unknown metrics"
	}
}

type IMetricsHandler interface {
	AddCounter(mn MetricName) error
	AddGauge(mn MetricName) error
	AddGaugeFunction(mn MetricName, mf func() float64) error
	AddHistogram(mn MetricName) error

	// Increments an existing metric by one. The metric must be a counter or a gauge.
	// An error is returned if the metric does not exist, or is not a counter nor a gauge.
	IncrementCountByOne(mn MetricName) error

	// Decrements an existing metric by one. The metric must be a gauge.
	// An error is returned if the metric does not exist, or is not a gauge.
	DecrementCountByOne(mn MetricName) error

	// Adds the given value to an existing metric. The metric can be either a counter or a gauge.
	// An error is returned if the metric does not exist, or is not a counter nor a gauge; an error is
	// also returned if the metric is a counter, and valueToAdd is negative.
	AddToCount(mn MetricName, valueToAdd int) error

	// Subtracts the given value to an existing metric. The metric must be a gauge.
	// An error is returned if the metric does not exist, or is not a gauge.
	SubtractFromCount(mn MetricName, valueToSubtract int) error

	// Tracks startTime, or more precisely, the duration obtained from time.Since(startTime).
	// An error is returned if the metric does not exist, or is not a histogram.
	TrackInHistogram(mn MetricName, startTime time.Time) error

	// Unregisters all registered metrics and discards all internal references to them.
	// An error is returned if at least one metric could not be unregistered.
	UnregisterAllMetrics() error
}

func DefaultHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "Proxy metrics haven't been initialized yet.", http.StatusServiceUnavailable)
	})
}
