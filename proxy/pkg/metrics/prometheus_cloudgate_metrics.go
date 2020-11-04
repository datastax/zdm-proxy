package metrics

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type PrometheusCloudgateProxyMetrics struct {
	collectorMap map[MetricsName]prometheus.Collector
	lock         *sync.RWMutex
}

/***
	Instantiation and initialization
 ***/

func NewPrometheusCloudgateProxyMetrics() *PrometheusCloudgateProxyMetrics {
	m := &PrometheusCloudgateProxyMetrics{
		collectorMap: make(map[MetricsName]prometheus.Collector),
		lock:         &sync.RWMutex{},
	}
	return m
}

/***
	Methods for adding metrics
 ***/

func (pm *PrometheusCloudgateProxyMetrics) AddCounter(mn MetricsName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: string(mn),
		Help: getMetricsDescription(mn),
	})
	pm.collectorMap[mn] = c
	return pm.registerCollector(c)
}

func (pm *PrometheusCloudgateProxyMetrics) AddGauge(mn MetricsName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: string(mn),
		Help: getMetricsDescription(mn),
	})
	pm.collectorMap[mn] = g
	return pm.registerCollector(g)
}

func (pm *PrometheusCloudgateProxyMetrics) AddGaugeFunction(mn MetricsName, mf func() float64) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	gf := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: string(mn),
			Help: getMetricsDescription(mn),
		},
		mf,
	)
	pm.collectorMap[mn] = gf
	return pm.registerCollector(gf)
}

func (pm *PrometheusCloudgateProxyMetrics) AddHistogram(mn MetricsName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    string(mn),
		Help:    getMetricsDescription(mn),
		Buckets: []float64{15, 30, 60, 90, 120, 200}, // TODO define latency buckets in some way that makes sense
	})
	pm.collectorMap[mn] = h
	return pm.registerCollector(h)
}

/***
	Methods that implement the IMetricsHandler interface:
		IncrementCountByOne(mn MetricsName) error
		DecrementCountByOne(mn MetricsName) error
		AddToCount(mn MetricsName, valueToAdd int) error
		SubtractFromCount(mn MetricsName, valueToSubtract int) error

		TrackInHistogram(mn MetricsName, timeToTrack time.Time) error

		UnregisterAllMetrics() error
 ***/

func (pm *PrometheusCloudgateProxyMetrics) IncrementCountByOne(mn MetricsName) error {
	var c prometheus.Collector
	var err error

	if c, err = pm.getCounterFromMap(mn); err == nil {
		// it is a counter: increment it by one
		c.(prometheus.Counter).Inc()
	} else {
		if c, err = pm.getGaugeFromMap(mn); err == nil {
			// it is a gauge: increment it by one
			c.(prometheus.Gauge).Inc()
		} else {
			return err
		}
	}
	return err
}

func (pm *PrometheusCloudgateProxyMetrics) DecrementCountByOne(mn MetricsName) error {
	if g, err := pm.getGaugeFromMap(mn); err == nil {
		g.Dec()
		return nil
	} else {
		return err
	}
}

func (pm *PrometheusCloudgateProxyMetrics) AddToCount(mn MetricsName, valueToAdd int) error {
	if g, err := pm.getGaugeFromMap(mn); err == nil {
		g.Add(float64(valueToAdd))
		return nil
	} else {
		return err
	}
}

func (pm *PrometheusCloudgateProxyMetrics) SubtractFromCount(mn MetricsName, valueToSubtract int) error {
	if g, err := pm.getGaugeFromMap(mn); err == nil {
		g.Sub(float64(valueToSubtract))
		return nil
	} else {
		return err
	}
}

func (pm *PrometheusCloudgateProxyMetrics) TrackInHistogram(mn MetricsName, begin time.Time) error {
	if h, err := pm.getHistogramFromMap(mn); err == nil {
		h.Observe(float64(time.Since(begin)) / float64(time.Second))
		return nil
	} else {
		return err
	}
}

func (pm *PrometheusCloudgateProxyMetrics) UnregisterAllMetrics() error {

	pm.lock.RLock()
	defer pm.lock.RUnlock()

	failed := false
	for mn, c := range pm.collectorMap {
		ok := prometheus.Unregister(c)
		if !ok {
			log.Errorf("Collector %s could not be found, unregister failed", mn)
			failed = true
		} else {
			log.Debugf("Collector %s successfully unregistered", mn)
		}
	}

	if failed {
		return errors.New("at least one collector failed to unregister")
	}
	return nil
}

/***
	Methods for internal use only
 ***/

func (pm *PrometheusCloudgateProxyMetrics) getCounterFromMap(mn MetricsName) (prometheus.Counter, error) {

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

func (pm *PrometheusCloudgateProxyMetrics) getGaugeFromMap(mn MetricsName) (prometheus.Gauge, error) {

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

func (pm *PrometheusCloudgateProxyMetrics) getHistogramFromMap(mn MetricsName) (prometheus.Histogram, error) {

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

	if err := prometheus.Register(c); err != nil {
		log.Errorf("Collector %s could not be registered due to %s", c, err)
		return err
	} else {
		log.Debugf("Collector %s registered", c)
	}
	return nil
}
