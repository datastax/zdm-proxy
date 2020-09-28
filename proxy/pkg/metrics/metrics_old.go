package metrics

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type MetricsOld struct {
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

// New returns a new MetricsOld struct for the port it is given.
func New(port int) *MetricsOld {
	return &MetricsOld{
		lock: &sync.Mutex{},
		port: port,
	}
}

// Expose exposes the port associated with the MetricsOld struct.
func (m *MetricsOld) Expose() {
	go func() {
		http.HandleFunc("/", m.write)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", m.port), nil))
	}()
}

// write Marshals the MetricsOld struct and writes it to the repsonse.
func (m *MetricsOld) write(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	marshaled, err := json.Marshal(m)
	if err != nil {
		w.Write([]byte(`{"error": "unable to grab metrics"}`))
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(marshaled)
}

func (m *MetricsOld) IncrementFrames() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.FrameCount++
}

func (m *MetricsOld) IncrementReads() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Reads++
}

func (m *MetricsOld) IncrementWrites() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Writes++
}

func (m *MetricsOld) IncrementWriteFails() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.WriteFails++
}

func (m *MetricsOld) IncrementReadFails() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ReadFails++
}

func (m *MetricsOld) IncrementServerErrors() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ServerErrors++
}

func (m *MetricsOld) IncrementConnections() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ConnectionsToSource++
}

func (m *MetricsOld) DecrementConnections() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ConnectionsToSource--
}

func (m *MetricsOld) SourceConnections() int {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.ConnectionsToSource
}
