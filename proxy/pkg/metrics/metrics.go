package metrics

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
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

// New returns a new Metrics struct for the port it is given.
func New(port int) *Metrics {
	return &Metrics{
		lock: &sync.Mutex{},
		port: port,
	}
}

// Expose exposes the port associated with the Metrics struct.
func (m *Metrics) Expose() {
	go func() {
		http.HandleFunc("/", m.write)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", m.port), nil))
	}()
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
