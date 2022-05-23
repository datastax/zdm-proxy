package metrics

import (
	"fmt"
	"net/http"
	"sync"
)

type nodeMetricsBuilder = func(
	metricFactory MetricFactory,
	nodeIdentifier string,
	buckets []float64) (*NodeMetricsInstance, error)

type MetricHandler struct {
	originMetricsBuilder nodeMetricsBuilder
	targetMetricsBuilder nodeMetricsBuilder
	asyncMetricsBuilder  nodeMetricsBuilder

	proxyMetrics *ProxyMetrics

	originMetrics map[string]*NodeMetricsInstance
	targetMetrics map[string]*NodeMetricsInstance
	asyncMetrics  map[string]*NodeMetricsInstance

	originRwLock *sync.RWMutex
	targetRwLock *sync.RWMutex
	asyncRwLock  *sync.RWMutex

	metricFactory MetricFactory

	originBuckets []float64
	targetBuckets []float64
	asyncBuckets  []float64
}

func NewMetricHandler(
	metricFactory MetricFactory,
	originBuckets []float64,
	targetBuckets []float64,
	asyncBuckets []float64,
	proxyMetrics *ProxyMetrics,
	originMetricsBuilder nodeMetricsBuilder,
	targetMetricsBuilder nodeMetricsBuilder,
	asyncMetricsBuilder nodeMetricsBuilder) *MetricHandler {
	return &MetricHandler{
		originMetricsBuilder: originMetricsBuilder,
		targetMetricsBuilder: targetMetricsBuilder,
		asyncMetricsBuilder:  asyncMetricsBuilder,
		proxyMetrics:         proxyMetrics,
		originMetrics:        make(map[string]*NodeMetricsInstance),
		targetMetrics:        make(map[string]*NodeMetricsInstance),
		asyncMetrics:         make(map[string]*NodeMetricsInstance),
		originRwLock:         &sync.RWMutex{},
		targetRwLock:         &sync.RWMutex{},
		asyncRwLock:          &sync.RWMutex{},
		metricFactory:        metricFactory,
		originBuckets:        originBuckets,
		targetBuckets:        targetBuckets,
		asyncBuckets:         asyncBuckets,
	}
}

func (recv *MetricHandler) GetProxyMetrics() *ProxyMetrics {
	return recv.proxyMetrics
}

func (recv *MetricHandler) getOriginMetrics(originNodeDescription string) (*NodeMetricsInstance, error) {
	rwLock := recv.originRwLock
	m := recv.originMetrics
	buckets := recv.originBuckets
	builder := recv.originMetricsBuilder

	rwLock.RLock()
	originMetrics, ok := m[originNodeDescription]
	rwLock.RUnlock()
	if ok {
		return originMetrics, nil
	}

	rwLock.Lock()
	originMetrics, ok = m[originNodeDescription]
	if ok {
		rwLock.Unlock()
		return originMetrics, nil
	}

	newNodeMetrics, err := builder(recv.metricFactory, originNodeDescription, buckets)
	if err != nil {
		rwLock.Unlock()
		return nil, fmt.Errorf("failed to create origin metrics: %w", err)
	}

	m[originNodeDescription] = newNodeMetrics
	rwLock.Unlock()
	return newNodeMetrics, nil
}

func (recv *MetricHandler) getTargetMetrics(targetNodeDescription string) (*NodeMetricsInstance, error) {
	rwLock := recv.targetRwLock
	m := recv.targetMetrics
	buckets := recv.targetBuckets
	builder := recv.targetMetricsBuilder

	rwLock.RLock()
	targetMetrics, ok := m[targetNodeDescription]
	rwLock.RUnlock()
	if ok {
		return targetMetrics, nil
	}

	rwLock.Lock()
	targetMetrics, ok = m[targetNodeDescription]
	if ok {
		rwLock.Unlock()
		return targetMetrics, nil
	}

	newNodeMetrics, err := builder(recv.metricFactory, targetNodeDescription, buckets)
	if err != nil {
		rwLock.Unlock()
		return nil, fmt.Errorf("failed to create async metrics: %w", err)
	}

	m[targetNodeDescription] = newNodeMetrics
	rwLock.Unlock()
	return newNodeMetrics, nil
}

func (recv *MetricHandler) getAsyncMetrics(asyncNodeDescription string) (*NodeMetricsInstance, error) {
	rwLock := recv.asyncRwLock
	m := recv.asyncMetrics
	buckets := recv.asyncBuckets
	builder := recv.asyncMetricsBuilder

	rwLock.RLock()
	asyncMetrics, ok := m[asyncNodeDescription]
	rwLock.RUnlock()
	if ok {
		return asyncMetrics, nil
	}

	rwLock.Lock()
	asyncMetrics, ok = m[asyncNodeDescription]
	if ok {
		rwLock.Unlock()
		return asyncMetrics, nil
	}

	newNodeMetrics, err := builder(recv.metricFactory, asyncNodeDescription, buckets)
	if err != nil {
		rwLock.Unlock()
		return nil, fmt.Errorf("failed to create target metrics: %w", err)
	}

	m[asyncNodeDescription] = newNodeMetrics
	rwLock.Unlock()
	return newNodeMetrics, nil
}

func (recv *MetricHandler) GetNodeMetrics(
	originNodeDescription string, targetNodeDescription string, asyncNodeDescription string) (*NodeMetrics, error) {
	originMetrics, err := recv.getOriginMetrics(originNodeDescription)
	if err != nil {
		return nil, err
	}

	targetMetrics, err := recv.getTargetMetrics(targetNodeDescription)
	if err != nil {
		return nil, err
	}

	var asyncMetrics *NodeMetricsInstance
	if asyncNodeDescription != "" {
		asyncMetrics, err = recv.getAsyncMetrics(asyncNodeDescription)
		if err != nil {
			return nil, err
		}
	}

	return &NodeMetrics{OriginMetrics: originMetrics, TargetMetrics: targetMetrics, AsyncMetrics: asyncMetrics}, nil
}

func (recv *MetricHandler) UnregisterAllMetrics() error {
	return recv.metricFactory.UnregisterAllMetrics()
}

func (recv *MetricHandler) GetHttpHandler() http.Handler {
	return recv.metricFactory.HttpHandler()
}