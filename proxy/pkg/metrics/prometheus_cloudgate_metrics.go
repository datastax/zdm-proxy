package metrics

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"sync"
)

type PrometheusCloudgateProxyMetrics struct {
	collectorMap map[MetricsName]prometheus.Collector
	lock         *sync.RWMutex
}

/***
	Instantiation and initialization
 ***/

func NewPrometheusCloudgateProxyMetrics() IMetricsHandler {
	m := &PrometheusCloudgateProxyMetrics{
		collectorMap: make(map[MetricsName]prometheus.Collector),
		lock:         &sync.RWMutex{},
	}
	m.initialize()
	return m
}

func (pm *PrometheusCloudgateProxyMetrics) initialize() {
	pm.addGauge(SuccessReadCount)
	pm.addGauge(FailedReadCount)
	pm.addGauge(SuccessWriteCount)
	pm.addGauge(FailedOriginWriteCount)
	pm.addGauge(FailedTargetWriteCount)
	pm.addGauge(FailedBothWriteCount)
	pm.addGauge(PSCacheMissCount)
	pm.addGauge(UnpreparedReadCount)
	pm.addGauge(UnpreparedOriginWriteCount)
	pm.addGauge(UnpreparedTargetWriteCount)
	pm.addGauge(SuccessBatchCount)
	pm.addGauge(FailedBatchCount)

	pm.addHistogram(ProxyReadLatencyHist)
	pm.addHistogram(OriginReadLatencyHist)
	pm.addHistogram(ProxyWriteLatencyHist)
	pm.addHistogram(OriginWriteLatencyHist)
	pm.addHistogram(TargetWriteLatencyHist)

	pm.addGauge(CurrentRequestCount)
	pm.addGauge(CurrentGoroutineCount)
	pm.addGauge(CurrentClientConnCount)
	pm.addGauge(CurrentOriginConnCount)
	pm.addGauge(CurrentTargetConnCount)
}

/***
	Methods that implement the IMetricsHandler interface:
		IncrementCountByOne(name MetricsName) error
		DecrementCountByOne(name MetricsName) error
		AddToCount(name MetricsName, valueToAdd int) error
		SubtractFromCount(name MetricsName, valueToSubtract int) error
		ResetCount(name MetricsName) error

		TrackInHistogram(name MetricsName, valueToTrack float64) error
		ResetHistogram(name MetricsName) error
 ***/

func (pm *PrometheusCloudgateProxyMetrics) IncrementCountByOne(mn MetricsName) error {
	if g, err := pm.getGaugeFromMap(mn); err == nil {
		g.Inc()
		return nil
	} else {
		return err
	}
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

func (pm *PrometheusCloudgateProxyMetrics) TrackInHistogram(mn MetricsName, valueToTrack float64) error {
	if h, err := pm.getHistogramFromMap(mn); err == nil {
		h.Observe(valueToTrack)
		return nil
	} else {
		return err
	}
}

/***
	Methods for internal use only
	TODO decide if we should use counters or just gauges
 ***/

//func (pm *PrometheusCloudgateProxyMetrics) addCounter(mn MetricsName) error {
//	pm.lock.Lock()
//	defer pm.lock.Unlock()
//
//	c := prometheus.NewCounter(prometheus.CounterOpts{
//		Name:        string(mn),
//		Help:        getMetricsDescription(mn),
//	})
//	pm.collectorMap[mn] = c
//	return pm.registerCollector(c)
//}

func (pm *PrometheusCloudgateProxyMetrics) addGauge(mn MetricsName) error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: string(mn),
		Help: getMetricsDescription(mn),
	})
	pm.collectorMap[mn] = g
	return pm.registerCollector(g)
}

func (pm *PrometheusCloudgateProxyMetrics) addHistogram(mn MetricsName) error {
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

//func (pm *PrometheusCloudgateProxyMetrics) getCounterFromMap(mn MetricsName) (prometheus.Counter, error) {
//	c, foundInMap := pm.collectorMap[mn]
//	if !foundInMap {
//		log.Errorf("No counter could be found with name %s", mn)
//		return nil, errors.New("counter not found")
//	}
//
//	if ct, isCounter := c.(prometheus.Counter); !isCounter {
//		log.Errorf("The specified metrics %s is not a counter", mn)
//		return nil, errors.New("the specified metrics is not a counter")
//	} else {
//		return ct, nil
//	}
//}

func (pm *PrometheusCloudgateProxyMetrics) getGaugeFromMap(mn MetricsName) (prometheus.Gauge, error) {
	c, foundInMap := pm.collectorMap[mn]
	if !foundInMap {
		log.Errorf("No gauge could be found with name %s", mn)
		return nil, errors.New("gauge not found")
	}

	if g, isGauge := c.(prometheus.Gauge); !isGauge {
		log.Errorf("The specified metrics %s is not a gauge", mn)
		return nil, errors.New("the specified metrics is not a gauge")
	} else {
		return g, nil
	}
}

func (pm *PrometheusCloudgateProxyMetrics) getHistogramFromMap(mn MetricsName) (prometheus.Histogram, error) {
	c, foundInMap := pm.collectorMap[mn]
	if !foundInMap {
		log.Errorf("No histogram could be found with name %s", mn)
		return nil, errors.New("histogram not found")
	}

	if h, isHistogram := c.(prometheus.Histogram); !isHistogram {
		log.Errorf("The specified metrics %s is not a histogram", mn)
		return nil, errors.New("the specified metrics is not a histogram")
	} else {
		return h, nil
	}
}

// Register this collector with Prometheus's DefaultRegisterer.
func (pm *PrometheusCloudgateProxyMetrics) registerCollector(c prometheus.Collector) error {
	if err := prometheus.Register(c); err != nil {
		log.Errorf("Collector %s could not be registered due to @s", c, err)
		return err
	} else {
		log.Debugf("Collector %s registered", c)
	}
	return nil
}
