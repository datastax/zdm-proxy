package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusCloudgateProxyMetrics struct {

	readRequests *prometheus.CounterVec
}

