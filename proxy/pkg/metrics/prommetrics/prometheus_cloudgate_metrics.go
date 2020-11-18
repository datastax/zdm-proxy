package prommetrics

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type PrometheusCloudgateProxyMetrics struct {
	collectorMap map[metrics.MetricName]prometheus.Collector
	lock         *sync.RWMutex
	registerer   prometheus.Registerer
}

/***
	Instantiation and initialization
 ***/

func NewPrometheusCloudgateProxyMetrics(registerer prometheus.Registerer) *PrometheusCloudgateProxyMetrics {
	m := &PrometheusCloudgateProxyMetrics{
		collectorMap: make(map[metrics.MetricName]prometheus.Collector),
		lock:         &sync.RWMutex{},
		registerer:   registerer,
	}
	return m
}

/***
	Methods for adding metrics
 ***/

func (pm *PrometheusCloudgateProxyMetrics) AddCounter(mn metrics.MetricName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: string(mn),
		Help: mn.GetDescription(),
	})
	pm.collectorMap[mn] = c
	return pm.registerCollector(c)
}

func (pm *PrometheusCloudgateProxyMetrics) AddGauge(mn metrics.MetricName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: string(mn),
		Help: mn.GetDescription(),
	})
	pm.collectorMap[mn] = g
	return pm.registerCollector(g)
}

func (pm *PrometheusCloudgateProxyMetrics) AddGaugeFunction(mn metrics.MetricName, mf func() float64) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	gf := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: string(mn),
			Help: mn.GetDescription(),
		},
		mf,
	)
	pm.collectorMap[mn] = gf
	return pm.registerCollector(gf)
}

func (pm *PrometheusCloudgateProxyMetrics) AddHistogram(mn metrics.MetricName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    string(mn),
		Help:    mn.GetDescription(),
		Buckets: []float64{15, 30, 60, 90, 120, 200}, // TODO define latency buckets in some way that makes sense
	})
	pm.collectorMap[mn] = h
	return pm.registerCollector(h)
}

func (pm *PrometheusCloudgateProxyMetrics) IncrementCountByOne(mn metrics.MetricName) error {
	return pm.AddToCount(mn, 1)
}

func (pm *PrometheusCloudgateProxyMetrics) DecrementCountByOne(mn metrics.MetricName) error {
	return pm.SubtractFromCount(mn, 1)
}

func (pm *PrometheusCloudgateProxyMetrics) AddToCount(mn metrics.MetricName, valueToAdd int) error {
	var c prometheus.Collector
	var err error
	if c, err = pm.getCounterFromMap(mn); err == nil {
		c.(prometheus.Counter).Add(float64(valueToAdd))
	} else if c, err = pm.getGaugeFromMap(mn); err == nil {
		c.(prometheus.Gauge).Add(float64(valueToAdd))
	}
	return err
}

func (pm *PrometheusCloudgateProxyMetrics) SubtractFromCount(mn metrics.MetricName, valueToSubtract int) error {
	var c prometheus.Collector
	var err error
	if c, err = pm.getGaugeFromMap(mn); err == nil {
		c.(prometheus.Gauge).Sub(float64(valueToSubtract))
	}
	return err
}

func (pm *PrometheusCloudgateProxyMetrics) TrackInHistogram(mn metrics.MetricName, begin time.Time) error {
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

	failed := false
	for mn, c := range pm.collectorMap {
		ok := pm.registerer.Unregister(c)
		if !ok {
			log.Errorf("Collector %s could not be found, unregister failed", mn)
			failed = true
		} else {
			log.Debugf("Collector %s successfully unregistered", mn)
		}
		delete(pm.collectorMap, mn)
	}

	if failed {
		return errors.New("at least one collector failed to unregister")
	}
	return nil
}

/***
	Methods for internal use only
 ***/

func (pm *PrometheusCloudgateProxyMetrics) getCounterFromMap(mn metrics.MetricName) (prometheus.Counter, error) {

	pm.lock.RLock()
	defer pm.lock.RUnlock()

	if c, foundInMap := pm.collectorMap[mn]; foundInMap {
		if ct, isCounter := c.(prometheus.Counter); isCounter {
			return ct, nil
		} else {
			log.Errorf("The specified metrics %s is not a counter", mn)
			return nil, errors.New("the specified metrics is not a counter")
		}
	} else {
		// collector not found
		log.Errorf("Metrics %s could not be found", mn)
		return nil, errors.New("the specified metrics could not be found")
	}
}

func (pm *PrometheusCloudgateProxyMetrics) getGaugeFromMap(mn metrics.MetricName) (prometheus.Gauge, error) {

	pm.lock.RLock()
	defer pm.lock.RUnlock()

	if c, foundInMap := pm.collectorMap[mn]; foundInMap {
		if g, isGauge := c.(prometheus.Gauge); isGauge {
			return g, nil
		} else {
			log.Errorf("The specified metrics %s is not a gauge", mn)
			return nil, errors.New("the specified metrics is not a gauge")
		}
	} else {
		// collector not found
		log.Errorf("Metrics %s could not be found", mn)
		return nil, errors.New("the specified metrics could not be found")
	}
}

func (pm *PrometheusCloudgateProxyMetrics) getHistogramFromMap(mn metrics.MetricName) (prometheus.Histogram, error) {

	pm.lock.RLock()
	defer pm.lock.RUnlock()

	if c, foundInMap := pm.collectorMap[mn]; foundInMap {
		if h, isHistogram := c.(prometheus.Histogram); isHistogram {
			return h, nil
		} else {
			log.Errorf("The specified metrics %s is not a histogram", mn)
			return nil, errors.New("the specified metrics is not a histogram")
		}
	} else {
		// collector not found
		log.Errorf("Metrics %s could not be found", mn)
		return nil, errors.New("the specified metrics could not be found")
	}
}

// Register this collector with Prometheus's DefaultRegisterer.
func (pm *PrometheusCloudgateProxyMetrics) registerCollector(c prometheus.Collector) error {

	if err := pm.registerer.Register(c); err != nil {
		log.Errorf("Collector %s could not be registered due to %s", c, err)
		return err
	} else {
		log.Debugf("Collector %s registered", c)
	}
	return nil
}
