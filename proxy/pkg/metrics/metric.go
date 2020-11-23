package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

type metric struct {
	name                 string
	labels               map[string]string
	description          string
	stringRepresentation string
	identifier           uint32
}

type Metric interface {
	GetName() string
	GetLabels() map[string]string
	GetDescription() string
	GetUniqueIdentifier() uint32
	String() string
}

type histogramMetric struct {
	*metric
	buckets []float64
}

type HistogramMetric interface {
	Metric
	GetBuckets() []float64
}

var (
	metricIdentifierCounter uint32 = 0
	defaultHistogramBuckets        = []float64{15, 30, 60, 90, 120, 200} // TODO define latency buckets in some way that makes sense
)

func incrementMetricIdentifier() uint32 {
	return atomic.AddUint32(&metricIdentifierCounter, 1)
}

func newMetricBase(name string, description string, labels map[string]string) *metric {
	m := &metric{
		name:        name,
		description: description,
		labels:      labels,
		identifier:  incrementMetricIdentifier(),
	}
	m.stringRepresentation = computeStringRepresentation(m)
	return m
}

func NewMetric(name string, description string) Metric {
	return newMetricBase(name, description, nil)
}

func NewMetricWithLabels(name string, description string, labels map[string]string) Metric {
	return newMetricBase(name, description, labels)
}

func NewHistogramMetric(name string, description string, buckets []float64) HistogramMetric {
	m := newMetricBase(name, description, nil)
	return &histogramMetric{
		metric:  m,
		buckets: buckets,
	}
}

func NewHistogramMetricWithLabels(
	name string, description string, buckets []float64, labels map[string]string) HistogramMetric {

	m := newMetricBase(name, description, labels)
	return &histogramMetric{
		metric:  m,
		buckets: buckets,
	}
}

func computeStringRepresentation(mn *metric) string {
	labels := mn.GetLabels()
	if labels != nil {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb := strings.Builder{}
		first := true
		for _, k := range keys {
			if !first {
				sb.WriteString(",")
			} else {
				first = false
			}
			sb.WriteString(k)
			sb.WriteString("=\"")
			sb.WriteString(labels[k])
			sb.WriteString("\"")
		}
		return fmt.Sprintf("%v{%v}", mn.GetName(), sb.String())
	}

	return fmt.Sprintf("%v", mn.GetName())
}

func (mn *metric) GetUniqueIdentifier() uint32 {
	return mn.identifier
}

func (mn *metric) String() string {
	return mn.stringRepresentation
}

func (mn *metric) GetName() string {
	return mn.name
}

func (mn *metric) GetLabels() map[string]string {
	return mn.labels
}

func (mn *metric) GetDescription() string {
	return mn.description
}

func (hmn *histogramMetric) GetBuckets() []float64 {
	return hmn.buckets
}