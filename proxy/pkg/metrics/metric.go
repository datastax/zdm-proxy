package metrics

import (
	"fmt"
	"sort"
	"strings"
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
	String() string
	WithLabels(map[string]string) Metric
}

func newMetricBase(name string, description string, labels map[string]string) *metric {
	m := &metric{
		name:        name,
		description: description,
		labels:      labels,
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

func (mn *metric) WithLabels(labels map[string]string) Metric {
	newLabels := make(map[string]string)
	for key, val := range mn.labels {
		newLabels[key] = val
	}
	for key, val := range labels {
		newLabels[key] = val
	}
	return NewMetricWithLabels(mn.name, mn.description, newLabels)
}
