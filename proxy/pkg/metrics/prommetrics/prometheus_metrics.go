package prommetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type PrometheusCounter struct {
	c prometheus.Counter
}

func (recv *PrometheusCounter) Add(valueToAdd int) {
	recv.c.Add(float64(valueToAdd))
}

type PrometheusGauge struct {
	g prometheus.Gauge
}

func (recv *PrometheusGauge) Add(valueToAdd int) {
	recv.g.Add(float64(valueToAdd))
}

func (recv *PrometheusGauge) Subtract(valueToSubtract int) {
	recv.g.Sub(float64(valueToSubtract))
}

type PrometheusGaugeFunc struct {
	gf prometheus.GaugeFunc
}

type PrometheusHistogram struct {
	h prometheus.Observer
}

func (recv *PrometheusHistogram) Track(begin time.Time) {
	// Use seconds to track time, see https://prometheus.io/docs/practices/naming/#base-units
	elapsedTimeInSeconds := float64(time.Since(begin)) / float64(time.Second)
	recv.h.Observe(elapsedTimeInSeconds)
}
