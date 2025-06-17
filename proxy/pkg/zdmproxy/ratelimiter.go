package zdmproxy

import (
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"golang.org/x/time/rate"
	"sync"
)

type RateLimitType string

const (
	RequestIdTracingLimit RateLimitType = "request-id-tracing-limit"
)

type RateLimiters struct {
	limiters *sync.Map
}

func InitializeRateLimiters(conf *config.Config) *RateLimiters {
	limiters := newRateLimiters()
	if conf.TracingEnabled && conf.TracingRateLimit > 0 {
		limiters.add(RequestIdTracingLimit, conf.TracingRateLimit, conf.TracingRateLimit)
	}
	return limiters
}

func (r *RateLimiters) Allow(key RateLimitType) bool {
	val, ok := r.limiters.Load(key)
	if !ok {
		return true
	}
	return val.(*rate.Limiter).Allow()
}

func newRateLimiters() *RateLimiters {
	return &RateLimiters{
		limiters: &sync.Map{},
	}
}

func (r *RateLimiters) add(key RateLimitType, limit int, burst int) {
	r.limiters.Store(key, rate.NewLimiter(rate.Limit(limit), burst))
}
