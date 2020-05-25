package migration

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Metrics contains characteristics about migration
type Metrics struct {
	TablesMigrated int
	TablesLeft     int
	Speed          float64
	SizeMigrated   float64

	lock      *sync.Mutex
	port      int
	directory string
}

// NewMetrics creates a new Metrics struct
func NewMetrics(port int, directory string, totalTables int) *Metrics {
	metrics := Metrics{
		TablesLeft: totalTables,
		lock:       &sync.Mutex{},
		port:       port,
		directory:  directory,
	}

	metrics.StartSpeedMetrics()

	return &metrics
}

// StartSpeedMetrics updates the speed and sizes of migration every second
func (m *Metrics) StartSpeedMetrics() {
	go func() {
		for {
			// Calculate size and speed in megabytes per second
			out, _ := exec.Command("aws", "s3", "ls", "--summarize", "--recursive", "s3://codebase-datastax-test/"+m.directory).Output()
			r, _ := regexp.Compile("Total Size: [0-9]+")
			match := r.FindString(string(out))

			numBytes, _ := strconv.ParseFloat(match[12:], 64)

			// In MB/s and MB, respectively
			m.Speed = numBytes/1024/1024 - m.SizeMigrated
			m.SizeMigrated = numBytes / 1024 / 1024

			time.Sleep(time.Second)
		}
	}()
}

// Expose exposes the endpoint for metrics
func (m *Metrics) Expose() {
	go func() {
		http.HandleFunc("/", m.write)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", m.port), nil))
	}()
}

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

// IncrementTablesMigrated increments tables that have been migrated
func (m *Metrics) IncrementTablesMigrated() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.TablesMigrated++
}

// DecrementTablesMigrated decrements tables that have been migrated
func (m *Metrics) DecrementTablesMigrated() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.TablesMigrated--
}

// IncrementTablesLeft increments number of tables that need to be migrated
func (m *Metrics) IncrementTablesLeft() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.TablesLeft++
}

// DecrementTablesLeft decrements number of tables that need to be migrated
func (m *Metrics) DecrementTablesLeft() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.TablesLeft--
}
