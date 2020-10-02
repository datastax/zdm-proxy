package metrics

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

	[Not sure about batches - are they worth counting separately? - leaving them out for now]
	 - Batches, successful
	 - Batches, failed

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
	SuccessReadCount = MetricsName("SuccessfulReadRequestCount")
	FailedReadCount  = MetricsName("FailedReadRequestCount")

	SuccessWriteCount      = MetricsName("SuccessfulWriteRequestCount")
	FailedOriginWriteCount = MetricsName("FailedOnOriginOnlyWriteRequestCount")
	FailedTargetWriteCount = MetricsName("FailedOnTargetOnlyWriteRequestCount")
	FailedBothWriteCount   = MetricsName("FailedOnBothWriteRequestCount")

	PSCacheMissCount           = MetricsName("PreparedStatementCacheMissCount")
	UnpreparedReadCount        = MetricsName("UnpreparedReadRequestCount")
	UnpreparedOriginWriteCount = MetricsName("UnpreparedWriteRequestOnOriginCount")
	UnpreparedTargetWriteCount = MetricsName("UnpreparedWriteRequestOnTargetCount")

	SuccessBatchCount = MetricsName("SuccessfulBatchRequestCount")
	FailedBatchCount  = MetricsName("FailedBatchRequestCount")

	ProxyReadLatencyHist   = MetricsName("ReadRequestProxyLatencyHist")
	OriginReadLatencyHist  = MetricsName("ReadRequestOriginLatencyHist")
	ProxyWriteLatencyHist  = MetricsName("ProxyWriteRequestLatencyHist")
	OriginWriteLatencyHist = MetricsName("OriginWriteRequestLatencyHist")
	TargetWriteLatencyHist = MetricsName("TargetWriteRequestLatencyHist")

	CurrentRequestCount    = MetricsName("CurrentClientRequestCount")
	CurrentGoroutineCount  = MetricsName("CurrentGoroutineCount")
	CurrentClientConnCount = MetricsName("CurrentClientConnectionCount")
	CurrentOriginConnCount = MetricsName("CurrentOriginConnectionCount")
	CurrentTargetConnCount = MetricsName("CurrentTargetConnectionCount")
)

func getMetricsDescription(mn MetricsName) string {
	switch mn {
	case SuccessReadCount:
		return "Running total of successful read requests"
	case FailedReadCount:
		return "Running total of failed read requests"
	case SuccessWriteCount:
		return "Running total of successful write requests"
	case FailedOriginWriteCount:
		return "Running total of write requests that failed on Origin Cassandra only"
	case FailedTargetWriteCount:
		return "Running total of write requests that failed on Target Cassandra only"
	case FailedBothWriteCount:
		return "Running total of write requests that failed on both clusters"
	case PSCacheMissCount:
		return "Running total of prepared statement cache misses in the proxy"
	case UnpreparedReadCount:
		return "Running total of PreparedStatement reads that were found to be unprepared on Origin Cassandra"
	case UnpreparedOriginWriteCount:
		return "Running total of PreparedStatement writes that were found to be unprepared on Origin Cassandra"
	case UnpreparedTargetWriteCount:
		return "Running total of PreparedStatement writes that were found to be unprepared on Target Cassandra"
	case SuccessBatchCount:
		return "Running total of successful batch requests"
	case FailedBatchCount:
		return "Running total of failed batch requests"
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
	case CurrentRequestCount:
		return "Number of client requests currently in flight in the proxy"
	case CurrentGoroutineCount:
		return "Number of goroutines currently running in the proxy"
	case CurrentClientConnCount:
		return "Number of client connections currently open"
	case CurrentOriginConnCount:
		return "Number of connections to Origin Cassandra currently open"
	case CurrentTargetConnCount:
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

	TrackInHistogram(mn MetricsName, valueToTrack float64) error
}
