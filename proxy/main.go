package main

import (
	"bufio"
	"cloud-gate/config"
	"cloud-gate/migration/migration"
	"cloud-gate/proxy/filter"
	"encoding/json"
	"net/http"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	p *filter.CQLProxy
)

// Method mainly to test the proxy service for now
func main() {
	conf := config.New().ParseEnvVars()

	if conf.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	p = &filter.CQLProxy{
		Conf: conf,
	}

	// for testing purposes. to delete
	if conf.Test {
		go doTesting(p)
	}

	err := p.Start()
	if err != nil {
		// TODO: handle error
		panic(err)
	}

	// start metrics
	go runMetrics()

	for {
		select {
		case <-p.ReadyForRedirect:
			log.Info("Coordinate received signal that there are no more connections to Client Database.")
		}
	}
}

func runMetrics() {
	http.HandleFunc("/metrics", getMetrics)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	marshaled, err := json.Marshal(p.Metrics)
	if err != nil {
		w.Write([]byte(`{"error": "unable to grab metrics"}`))
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(marshaled)
}

//function for testing purposes. Will be deleted later. toggles status for table 'codebase' upon user input
func doTesting(p *filter.CQLProxy) {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "start":
				tables := make(map[string]map[string]*migration.Table)
				tables["codebase"] = make(map[string]*migration.Table)
				tables["codebase"]["tasks"] = &migration.Table{
					Name:     "tasks",
					Keyspace: "codebase",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				p.MigrationStart <- &migration.Status{Tables: tables,
					Lock: &sync.Mutex{}}
			case "complete":
				p.MigrationDone <- struct{}{}
			case "shutdown":
				p.ShutdownChan <- struct{}{}
			case "restart":
				table := &migration.Table{
					Name:     "tasks",
					Keyspace: "codebase",
					Step:     migration.LoadingDataComplete,
					Error:    nil,

					Lock: &sync.Mutex{},
				}
				p.TableMigrated <- table
			}
		}
	}
}
