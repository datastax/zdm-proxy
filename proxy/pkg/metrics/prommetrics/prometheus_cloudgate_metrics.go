package prommetrics

import (
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

const metricsPrefix = "cloudgate"

type collectorEntry struct {
	metric    metrics.Metric
	collector prometheus.Collector
}

type PrometheusCloudgateProxyMetrics struct {
	collectorMap map[uint32]*collectorEntry
	lock         *sync.RWMutex
	registerer   prometheus.Registerer
}

/***
	Instantiation and initialization
 ***/

func NewPrometheusCloudgateProxyMetrics(registerer prometheus.Registerer) *PrometheusCloudgateProxyMetrics {
	m := &PrometheusCloudgateProxyMetrics{
		collectorMap: make(map[uint32]*collectorEntry),
		lock:         &sync.RWMutex{},
		registerer:   registerer,
	}
	return m
}

/***
	Methods for adding metrics
 ***/

func (pm *PrometheusCloudgateProxyMetrics) AddCounter(mn metrics.Metric) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	_, exists := pm.collectorMap[mn.GetUniqueIdentifier()]
	if exists {
		return fmt.Errorf("counter %v could not be registered: duplicate metrics collector registration attempted", mn)
	}

	var c prometheus.Collector
	if mn.GetLabels() != nil {
		c = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
			},
			getLabelNames(mn))
	} else {
		c = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsPrefix,
			Name: mn.GetName(),
			Help: mn.GetDescription(),
		})
	}

	var err error
	c, err = pm.registerCollector(mn, c)
	if err != nil {
		return fmt.Errorf("failed to add counter %v: %w", mn, err)
	}

	entry := &collectorEntry{
		metric:    mn,
		collector: c,
	}
	pm.collectorMap[mn.GetUniqueIdentifier()] = entry

	if mn.GetLabels() != nil {
		vec, isCounterVec := c.(*prometheus.CounterVec)
		if !isCounterVec {
			log.Warnf("failed to initialize label but collector was added: %v", mn)
			return nil
		}

		// initialize label
		_ = vec.With(mn.GetLabels())
	}

	return nil
}

func (pm *PrometheusCloudgateProxyMetrics) AddGauge(mn metrics.Metric) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	_, exists := pm.collectorMap[mn.GetUniqueIdentifier()]
	if exists {
		return fmt.Errorf("gauge %v could not be registered: duplicate metrics collector registration attempted", mn)
	}

	var g prometheus.Collector
	if mn.GetLabels() != nil {
		g = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
			},
			getLabelNames(mn))
	} else {
		g = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsPrefix,
			Name:      mn.GetName(),
			Help:      mn.GetDescription(),
		})
	}

	var err error
	g, err = pm.registerCollector(mn, g)
	if err != nil {
		return fmt.Errorf("failed to add gauge %v: %w", mn, err)
	}

	entry := &collectorEntry{
		metric:    mn,
		collector: g,
	}
	pm.collectorMap[mn.GetUniqueIdentifier()] = entry

	if mn.GetLabels() != nil {
		vec, isGaugeVec := g.(*prometheus.GaugeVec)
		if !isGaugeVec {
			log.Warnf("failed to initialize label but collector was added: %v", mn)
			return nil
		}

		// initialize label
		_ = vec.With(mn.GetLabels())
	}

	return nil
}

func (pm *PrometheusCloudgateProxyMetrics) AddGaugeFunction(mn metrics.Metric, mf func() float64) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	_, exists := pm.collectorMap[mn.GetUniqueIdentifier()]
	if exists {
		return fmt.Errorf("gauge function %v could not be registered: duplicate metrics collector registration attempted", mn)
	}

	var gf prometheus.Collector
	if mn.GetLabels() != nil {
		return fmt.Errorf("could not add metric %v because gauge functions don't support labels currently", mn)
	} else {
		gf = prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
			},
			mf,
		)
	}

	var err error
	gf, err = pm.registerCollector(mn, gf)
	if err != nil {
		return fmt.Errorf("failed to add gauge function %v: %w", mn, err)
	}

	entry := &collectorEntry{
		metric:    mn,
		collector: gf,
	}
	pm.collectorMap[mn.GetUniqueIdentifier()] = entry
	return nil
}

func (pm *PrometheusCloudgateProxyMetrics) AddHistogram(mn metrics.HistogramMetric) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	_, exists := pm.collectorMap[mn.GetUniqueIdentifier()]
	if exists {
		return fmt.Errorf("histogram %v could not be registered: duplicate metrics collector registration attempted", mn)
	}

	var h prometheus.Collector
	if mn.GetLabels() != nil {
		h = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
				Buckets:   mn.GetBuckets(),
			},
			getLabelNames(mn))
	} else {
		h = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   metricsPrefix,
			Name:        mn.GetName(),
			Help:        mn.GetDescription(),
			Buckets:     mn.GetBuckets(),
		})
	}

	var err error
	h, err = pm.registerCollector(mn, h)
	if err != nil {
		return fmt.Errorf("failed to add histogram %v: %w", mn, err)
	}

	entry := &collectorEntry{
		metric:    mn,
		collector: h,
	}
	pm.collectorMap[mn.GetUniqueIdentifier()] = entry

	if mn.GetLabels() != nil {
		vec, isHistogramVec := h.(*prometheus.HistogramVec)
		if !isHistogramVec {
			log.Warnf("failed to initialize label but collector was added: %v", mn)
			return nil
		}

		// initialize label
		_ = vec.With(mn.GetLabels())
	}

	return nil
}

func (pm *PrometheusCloudgateProxyMetrics) IncrementCountByOne(mn metrics.Metric) error {
	return pm.AddToCount(mn, 1)
}

func (pm *PrometheusCloudgateProxyMetrics) DecrementCountByOne(mn metrics.Metric) error {
	return pm.SubtractFromCount(mn, 1)
}

func (pm *PrometheusCloudgateProxyMetrics) AddToCount(mn metrics.Metric, valueToAdd int) error {
	c, err := pm.getMetricFromMap(mn)
	if err != nil {
		return err
	}

	switch ct := c.(type) {
	case prometheus.Counter:
		ct.Add(float64(valueToAdd))
	case prometheus.Gauge:
		ct.Add(float64(valueToAdd))
	default:
		return fmt.Errorf("the specified metric %v is neither a counter nor a gauge", mn)
	}

	return nil
}

func (pm *PrometheusCloudgateProxyMetrics) SubtractFromCount(mn metrics.Metric, valueToSubtract int) error {
	var c prometheus.Collector
	var err error
	if c, err = pm.getGaugeFromMap(mn); err == nil {
		c.(prometheus.Gauge).Sub(float64(valueToSubtract))
	}
	return err
}

func (pm *PrometheusCloudgateProxyMetrics) TrackInHistogram(mn metrics.Metric, begin time.Time) error {
	if h, err := pm.getHistogramFromMap(mn); err == nil {
		// Use seconds to track time, see https://prometheus.io/docs/practices/naming/#base-units
		elapsedTimeInSeconds := float64(time.Since(begin)) / float64(time.Second)
		h.Observe(elapsedTimeInSeconds)
		return nil
	} else {
		return err
	}
}

func (pm *PrometheusCloudgateProxyMetrics) UnregisterAllMetrics() error {

	pm.lock.Lock()
	defer pm.lock.Unlock()

	var failedUnregistrations []string
	unregisteredMetrics := map[string]interface{}{}

	for mn, c := range pm.collectorMap {
		if _, alreadyUnregistered := unregisteredMetrics[c.metric.GetName()]; !alreadyUnregistered {
			unregisteredMetrics[c.metric.GetName()] = nil
			ok := pm.registerer.Unregister(c.collector)
			if !ok {
				failedUnregistrations = append(failedUnregistrations, c.metric.String())
			} else {
				log.Debugf("Collector %v successfully unregistered.", c.metric.GetName())
			}
		}

		delete(pm.collectorMap, mn)
		log.Debugf("CollectorEntry for metric %v deleted from map.", c.metric.String())
	}

	if len(failedUnregistrations) > 0 {
		return fmt.Errorf(
			"failed to unregister the collectors of the following metrics: %v",
			strings.Join(failedUnregistrations, ", "))
	}

	return nil
}

/***
	Methods for internal use only
 ***/

func (pm *PrometheusCloudgateProxyMetrics) getCounterFromMap(mn metrics.Metric) (prometheus.Counter, error) {
	c, err := pm.getMetricFromMap(mn)
	if err != nil {
		return nil, err
	}

	if ct, isCounter := c.(prometheus.Counter); isCounter {
		return ct, nil
	} else {
		return nil, fmt.Errorf("the specified metric %v is not a counter", mn)
	}
}

func (pm *PrometheusCloudgateProxyMetrics) getGaugeFromMap(mn metrics.Metric) (prometheus.Gauge, error) {
	c, err := pm.getMetricFromMap(mn)
	if err != nil {
		return nil, err
	}

	if g, isGauge := c.(prometheus.Gauge); isGauge {
		return g, nil
	} else {
		return nil, fmt.Errorf("the specified metric %v is not a gauge", mn)
	}
}

func (pm *PrometheusCloudgateProxyMetrics) getHistogramFromMap(mn metrics.Metric) (prometheus.Histogram, error) {
	c, err := pm.getMetricFromMap(mn)
	if err != nil {
		return nil, err
	}

	if h, isHistogram := c.(prometheus.Histogram); isHistogram {
		return h, nil
	} else {
		return nil, fmt.Errorf("the specified metric %v is not a histogram", mn)
	}
}

func (pm *PrometheusCloudgateProxyMetrics) getMetricFromMap(mn metrics.Metric) (prometheus.Collector, error) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	if c, foundInMap := pm.collectorMap[mn.GetUniqueIdentifier()]; foundInMap {
		switch m := c.collector.(type) {
		case *prometheus.CounterVec:
			counter, err := m.GetMetricWith(mn.GetLabels())
			if err != nil {
				return nil, fmt.Errorf("failed to get counter from CounterVec: %w", err)
			} else {
				return counter, nil
			}
		case *prometheus.GaugeVec:
			gauge, err := m.GetMetricWith(mn.GetLabels())
			if err != nil {
				return nil, fmt.Errorf("failed to get gauge from GaugeVec: %w", err)
			} else {
				return gauge, nil
			}
		case *prometheus.HistogramVec:
			observer, err := m.GetMetricWith(mn.GetLabels())
			if err != nil {
				return nil, fmt.Errorf("failed to get histogram from HistogramVec: %w", err)
			}

			histogram, isHistogram := observer.(prometheus.Histogram)
			if isHistogram {
				return histogram, nil
			} else {
				return nil, fmt.Errorf("observer retrieved from metric %v isn't histogram", mn)
			}
		default:
			return c.collector, nil
		}
	} else {
		// collector not found
		return nil, fmt.Errorf("the specified metric %v could not be found", mn)
	}
}

// Register this collector with Prometheus's DefaultRegisterer.
// If it is a metric with labels and the registerer
// returns an AlreadyRegisteredError then the returned collector is the existing one (and no error is returned).
// If the registerer does not return any error, registerCollector returns c, i.e., the provided collector.
func (pm *PrometheusCloudgateProxyMetrics) registerCollector(mn metrics.Metric, c prometheus.Collector) (prometheus.Collector, error) {

	if err := pm.registerer.Register(c); err != nil {
		alreadyRegisteredErr := prometheus.AlreadyRegisteredError{}
		if errors.As(err, &alreadyRegisteredErr) && mn.GetLabels() != nil {
			// metrics with labels are only registered once instead of once per label combination
			return alreadyRegisteredErr.ExistingCollector, nil
		}
		return nil, fmt.Errorf("collector %v could not be registered due to %w", c, err)
	} else {
		log.Debugf("Collector %s registered", c)
	}
	return c, nil
}

func getLabelNames(mn metrics.Metric) []string {
	if mn.GetLabels() == nil {
		return nil
	}

	names := make([]string, 0, len(mn.GetLabels()))
	for name, _ := range mn.GetLabels() {
		names = append(names, name)
	}

	return names
}