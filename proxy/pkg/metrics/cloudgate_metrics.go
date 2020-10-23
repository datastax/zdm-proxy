package metrics

import "time"

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
type MetricsName string

const (
	SuccessReads = MetricsName("SuccessfulReadRequests")
	FailedReads  = MetricsName("FailedReadRequests")

	SuccessBothWrites      = MetricsName("SuccessfulOnBothWriteRequests")
	FailedOriginOnlyWrites = MetricsName("FailedOnOriginOnlyWriteRequests")
	FailedTargetOnlyWrites = MetricsName("FailedOnTargetOnlyWriteRequests")
	FailedBothWrites       = MetricsName("FailedOnBothWriteRequests")

	TimeOutsProxyOrigin        = MetricsName("RequestsTimedOutOnProxyFromOrigin")
	TimeOutsProxyTarget        = MetricsName("RequestsTimedOutOnProxyFromTarget")
	ReadTimeOutsOriginCluster  = MetricsName("ReadsTimedOutOnOriginCluster")
	WriteTimeOutsOriginCluster = MetricsName("WritesTimedOutOnOriginCluster")
	WriteTimeOutsTargetCluster = MetricsName("WritesTimedOutOnTargetCluster")

	UnpreparedReads        = MetricsName("UnpreparedReadRequestCount")
	UnpreparedOriginWrites = MetricsName("UnpreparedWriteRequestOnOriginCount")
	UnpreparedTargetWrites = MetricsName("UnpreparedWriteRequestOnTargetCount")
	PSCacheSize            = MetricsName("PreparedStatementCacheNumberOfEntries") // TODO
	PSCacheMissCount       = MetricsName("PreparedStatementCacheMissCount")


	ProxyReadLatencyHist   = MetricsName("ReadRequestProxyLatencyHist")
	OriginReadLatencyHist  = MetricsName("ReadRequestOriginLatencyHist")
	ProxyWriteLatencyHist  = MetricsName("ProxyWriteRequestLatencyHist")
	OriginWriteLatencyHist = MetricsName("OriginWriteRequestLatencyHist")
	TargetWriteLatencyHist = MetricsName("TargetWriteRequestLatencyHist")

	InFlightReadRequests  = MetricsName("InFlightReadRequests")
	InFlightWriteRequests = MetricsName("InFlightWriteRequests")
	OpenClientConnections = MetricsName("OpenClientConnections")
	OpenOriginConnections = MetricsName("OpenOriginConnections")
	OpenTargetConnections = MetricsName("OpenTargetConnections")
)

func getMetricsDescription(mn MetricsName) string {
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
	IncrementCountByOne(mn MetricsName) error
	DecrementCountByOne(mn MetricsName) error
	AddToCount(mn MetricsName, valueToAdd int) error
	SubtractFromCount(mn MetricsName, valueToSubtract int) error

	TrackInHistogram(mn MetricsName, timeToTrack time.Time) error

	UnregisterAllMetrics() error
}
