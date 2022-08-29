package httpzdmproxy

import (
	"net/http"
	"sync"
)

type HandlerWithFallback struct {
	rwMutex sync.RWMutex
	handler http.Handler
	defaultHandler http.Handler
}

func NewHandlerWithFallback(defaultHandler http.Handler) *HandlerWithFallback {
	return &HandlerWithFallback{
		rwMutex:        sync.RWMutex{},
		handler:        nil,
		defaultHandler: defaultHandler,
	}
}

func (h *HandlerWithFallback) SetHandler(handler http.Handler) {
	h.rwMutex.Lock()
	h.handler = handler
	h.rwMutex.Unlock()
}

func (h *HandlerWithFallback) ClearHandler() {
	h.rwMutex.Lock()
	h.handler = nil
	h.rwMutex.Unlock()
}

func (h *HandlerWithFallback) Handler() http.Handler {
	return http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		h.rwMutex.RLock()
		handler := h.handler
		h.rwMutex.RUnlock()

		if handler != nil {
			handler.ServeHTTP(rsp, req)
		} else {
			h.defaultHandler.ServeHTTP(rsp, req)
		}
	})
}