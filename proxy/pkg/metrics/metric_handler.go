package metrics

import (
	"fmt"
	"net/http"
	"sync"
)

type originMetricsBuilder = func(
	metricFactory MetricFactory,
	originNodeIdentifier string,
	originBuckets []float64) (*OriginMetrics, error)

type targetMetricsBuilder = func(
	metricFactory MetricFactory,
	targetNodeIdentifier string,
	targetBuckets []float64) (*TargetMetrics, error)

type MetricHandler struct {
	originMetricsBuilder originMetricsBuilder
	targetMetricsBuilder targetMetricsBuilder

	proxyMetrics *ProxyMetrics

	originMetrics map[string]*OriginMetrics
	targetMetrics map[string]*TargetMetrics

	originRwLock *sync.RWMutex
	targetRwLock *sync.RWMutex

	metricFactory MetricFactory

	originBuckets        []float64
	targetBuckets        []float64
}

func NewMetricHandler(
	metricFactory MetricFactory,
	originBuckets []float64,
	targetBuckets []float64,
	proxyMetrics *ProxyMetrics,
	originMetricsBuilder originMetricsBuilder,
	targetMetricsBuilder targetMetricsBuilder) *MetricHandler {
	return &MetricHandler{
		metricFactory:        metricFactory,
		originBuckets:        originBuckets,
		targetBuckets:        targetBuckets,
		originMetricsBuilder: originMetricsBuilder,
		targetMetricsBuilder: targetMetricsBuilder,
		originMetrics:        make(map[string]*OriginMetrics),
		targetMetrics:        make(map[string]*TargetMetrics),
		proxyMetrics:         proxyMetrics,
		originRwLock:         &sync.RWMutex{},
		targetRwLock:         &sync.RWMutex{},
	}
}

func (recv *MetricHandler) GetProxyMetrics() *ProxyMetrics {
	return recv.proxyMetrics
}

func (recv *MetricHandler) getOriginMetrics(originNodeDescription string) (*OriginMetrics, error) {
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

func (recv *MetricHandler) getTargetMetrics(targetNodeDescription string) (*TargetMetrics, error) {
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
		return nil, fmt.Errorf("failed to create target metrics: %w", err)
	}

	m[targetNodeDescription] = newNodeMetrics
	rwLock.Unlock()
	return newNodeMetrics, nil
}

func (recv *MetricHandler) GetNodeMetrics(originNodeDescription string, targetNodeDescription string) (*NodeMetrics, error) {
	originMetrics, err := recv.getOriginMetrics(originNodeDescription)
	if err != nil {
		return nil, err
	}

	targetMetrics, err := recv.getTargetMetrics(targetNodeDescription)
	if err != nil {
		return nil, err
	}

	return &NodeMetrics{OriginMetrics: originMetrics, TargetMetrics: targetMetrics}, nil
}

func (recv *MetricHandler) UnregisterAllMetrics() error {
	return recv.metricFactory.UnregisterAllMetrics()
}

func (recv *MetricHandler) GetHttpHandler() http.Handler {
	return recv.metricFactory.HttpHandler()
}