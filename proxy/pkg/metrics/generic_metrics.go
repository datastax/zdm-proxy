package metrics

import "time"

type Counter interface {
	Add(valueToAdd int)
}

type Gauge interface {
	Add(valueToAdd int)
	Subtract(valueToSubtract int)
}

type GaugeFunc interface {
}

type Histogram interface {
	Track(begin time.Time)
}