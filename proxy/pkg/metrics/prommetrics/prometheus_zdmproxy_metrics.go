package prommetrics

import (
	"errors"
	"fmt"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strings"
	"sync"
)

type PrometheusMetricFactory struct {
	registerer           prometheus.Registerer
	lock                 *sync.Mutex
	registeredCollectors []*collectorEntry
	metricsPrefix        string
}

/***
	Instantiation and initialization
 ***/

func NewPrometheusMetricFactory(registerer prometheus.Registerer, metricsPrefix string) *PrometheusMetricFactory {
	m := &PrometheusMetricFactory{
		registerer:           registerer,
		lock:                 &sync.Mutex{},
		registeredCollectors: make([]*collectorEntry, 0),
		metricsPrefix:        metricsPrefix,
	}
	return m
}

/***
	Methods for adding metrics
 ***/

func (pm *PrometheusMetricFactory) GetOrCreateCounter(mn metrics.Metric) (metrics.Counter, error) {

	var c prometheus.Collector
	if mn.GetLabels() != nil {
		c = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: pm.metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
			},
			getLabelNames(mn))
	} else {
		c = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: pm.metricsPrefix,
			Name:      mn.GetName(),
			Help:      mn.GetDescription(),
		})
	}

	var err error
	c, err = pm.registerCollector(mn, c)
	if err != nil {
		return nil, fmt.Errorf("failed to add counter %v: %w", mn, err)
	}

	if mn.GetLabels() != nil {
		vec, isCounterVec := c.(*prometheus.CounterVec)
		if !isCounterVec {
			return nil, fmt.Errorf("failed to initialize label but collector was added: %v", mn)
		}

		// initialize label
		promCounter := vec.With(mn.GetLabels())
		return &PrometheusCounter{c: promCounter}, nil
	} else {
		promCounter, isCounter := c.(prometheus.Counter)
		if !isCounter {
			return nil, fmt.Errorf("failed to convert prometheus counter but collector was added: %v", mn)
		}
		return &PrometheusCounter{c: promCounter}, nil
	}
}

func (pm *PrometheusMetricFactory) GetOrCreateGauge(mn metrics.Metric) (metrics.Gauge, error) {
	var g prometheus.Collector
	if mn.GetLabels() != nil {
		g = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pm.metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
			},
			getLabelNames(mn))
	} else {
		g = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: pm.metricsPrefix,
			Name:      mn.GetName(),
			Help:      mn.GetDescription(),
		})
	}

	var err error
	g, err = pm.registerCollector(mn, g)
	if err != nil {
		return nil, fmt.Errorf("failed to add gauge %v: %w", mn, err)
	}

	if mn.GetLabels() != nil {
		vec, isGaugeVec := g.(*prometheus.GaugeVec)
		if !isGaugeVec {
			return nil, fmt.Errorf("failed to initialize label but collector was added: %v", mn)
		}

		// initialize label
		promGauge := vec.With(mn.GetLabels())
		return &PrometheusGauge{g: promGauge}, nil
	} else {
		promGauge, isGauge := g.(prometheus.Gauge)
		if !isGauge {
			return nil, fmt.Errorf("failed to convert prometheus gauge but collector was added: %v", mn)
		}
		return &PrometheusGauge{g: promGauge}, nil
	}
}

func (pm *PrometheusMetricFactory) GetOrCreateGaugeFunc(mn metrics.Metric, mf func() float64) (metrics.GaugeFunc, error) {
	var gf prometheus.Collector
	if mn.GetLabels() != nil {
		return nil, fmt.Errorf("could not add metric %v because gauge functions don't support labels currently", mn)
	} else {
		gf = prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: pm.metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
			},
			mf,
		)
	}

	var err error
	gf, err = pm.registerCollector(mn, gf)
	if err != nil {
		return nil, fmt.Errorf("failed to add gauge function %v: %w", mn, err)
	}

	promGaugeFunc, isGaugeFunc := gf.(prometheus.GaugeFunc)
	if !isGaugeFunc {
		return nil, fmt.Errorf("failed to convert prometheus gauge func but collector was added: %v", mn)
	}
	return &PrometheusGaugeFunc{gf: promGaugeFunc}, nil
}

func (pm *PrometheusMetricFactory) GetOrCreateHistogram(mn metrics.Metric, buckets []float64) (metrics.Histogram, error) {

	var h prometheus.Collector
	if mn.GetLabels() != nil {
		h = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: pm.metricsPrefix,
				Name:      mn.GetName(),
				Help:      mn.GetDescription(),
				Buckets:   buckets,
			},
			getLabelNames(mn))
	} else {
		h = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: pm.metricsPrefix,
			Name:      mn.GetName(),
			Help:      mn.GetDescription(),
			Buckets:   buckets,
		})
	}

	var err error
	h, err = pm.registerCollector(mn, h)
	if err != nil {
		return nil, fmt.Errorf("failed to add histogram %v: %w", mn, err)
	}

	if mn.GetLabels() != nil {
		vec, isHistogramVec := h.(*prometheus.HistogramVec)
		if !isHistogramVec {
			return nil, fmt.Errorf("failed to initialize label but collector was added: %v", mn)
		}

		// initialize label
		promHistogram := vec.With(mn.GetLabels())
		return &PrometheusHistogram{h: promHistogram}, nil
	} else {
		promHistogram, isHistogram := h.(prometheus.Histogram)
		if !isHistogram {
			return nil, fmt.Errorf("failed to convert prometheus histogram func but collector was added: %v", mn)
		}
		return &PrometheusHistogram{h: promHistogram}, nil
	}
}

func (pm *PrometheusMetricFactory) UnregisterAllMetrics() error {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	var failedUnregistrations []string
	for _, c := range pm.registeredCollectors {
		ok := pm.registerer.Unregister(c.collector)
		if !ok {
			failedUnregistrations = append(failedUnregistrations, c.name)
		} else {
			log.Debugf("Collector %v successfully unregistered.", c.name)
		}
	}

	pm.registeredCollectors = make([]*collectorEntry, 0)

	if len(failedUnregistrations) > 0 {
		return fmt.Errorf(
			"failed to unregister the collectors of the following metrics: %v",
			strings.Join(failedUnregistrations, ", "))
	}

	return nil
}

func (pm *PrometheusMetricFactory) HttpHandler() http.Handler {
	return promhttp.Handler()
}

// Register this collector with Prometheus's DefaultRegisterer.
// If it is a metric with labels and the registerer
// returns an AlreadyRegisteredError then the returned collector is the existing one (and no error is returned).
// If the registerer does not return any error, registerCollector returns c, i.e., the provided collector.
func (pm *PrometheusMetricFactory) registerCollector(mn metrics.Metric, c prometheus.Collector) (prometheus.Collector, error) {
	if err := pm.registerer.Register(c); err != nil {
		alreadyRegisteredErr := prometheus.AlreadyRegisteredError{}
		if errors.As(err, &alreadyRegisteredErr) {
			return alreadyRegisteredErr.ExistingCollector, nil
		}
		return nil, fmt.Errorf("collector %v could not be registered due to %w", mn.String(), err)
	} else {
		log.Debugf("Collector %v registered", mn.GetName())
	}

	pm.lock.Lock()
	pm.registeredCollectors = append(pm.registeredCollectors, &collectorEntry{
		collector: c,
		name:      mn.GetName(),
	})
	pm.lock.Unlock()

	return c, nil
}

func getLabelNames(mn metrics.Metric) []string {
	if mn.GetLabels() == nil {
		return nil
	}

	names := make([]string, 0, len(mn.GetLabels()))
	for name := range mn.GetLabels() {
		names = append(names, name)
	}

	return names
}

type collectorEntry struct {
	collector prometheus.Collector
	name      string
}

func (recv *collectorEntry) String() string {
	return recv.name
}
