package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	FrameCount int

	Reads  int
	Writes int

	ServerErrors int
	WriteFails   int
	ReadFails    int

	ConnectionsToSource int

	lock *sync.Mutex
	port int
}

var globalMetrics = &atomic.Value{}
var globalMux = &sync.Mutex{}
var addedHandler = false

// New returns a new Metrics struct for the port it is given.
func New(port int) *Metrics {
	return &Metrics{
		lock: &sync.Mutex{},
		port: port,
	}
}

// Expose exposes the port associated with the Metrics struct.
func (m *Metrics) Expose(ctx context.Context) {
	go func() {
		setGlobalMetricsObject(m)
		globalMux.Lock()
		if !addedHandler {
			addedHandler = true
			http.HandleFunc("/", globalWrite)
		}
		globalMux.Unlock()

		srv := http.Server{}
		srv.Addr = fmt.Sprintf(":%d", m.port)

		go func() {
			<-ctx.Done()
			srv.Shutdown(context.Background())
		}()

		err := srv.ListenAndServe()
		if err == http.ErrServerClosed {
			log.Info("http server shutdown")
		} else {
			log.Fatalf("http server error: ", err)
		}
	}()
}

func setGlobalMetricsObject(m *Metrics) {
	globalMetrics.Store(m)
}

func globalWrite(w http.ResponseWriter, r *http.Request) {
	globalMetrics.Load().(*Metrics).write(w, r)
}

// write Marshals the Metrics struct and writes it to the repsonse.
func (m *Metrics) write(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	marshaled, err := json.Marshal(m)
	if err != nil {
		w.Write([]byte(`{"error": "unable to grab metrics"}`))
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(marshaled)
}

func (m *Metrics) IncrementFrames() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.FrameCount++
}

func (m *Metrics) IncrementReads() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Reads++
}

func (m *Metrics) IncrementWrites() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Writes++
}

func (m *Metrics) IncrementWriteFails() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.WriteFails++
}

func (m *Metrics) IncrementReadFails() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ReadFails++
}

func (m *Metrics) IncrementServerErrors() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ServerErrors++
}

func (m *Metrics) IncrementConnections() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ConnectionsToSource++
}

func (m *Metrics) DecrementConnections() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ConnectionsToSource--
}

func (m *Metrics) SourceConnections() int {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.ConnectionsToSource
}
