package main

import (
	"bufio"
	"cloud-gate/migration/migration"
	"cloud-gate/proxy/filter"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	sourceHostname string
	sourceUsername string
	sourcePassword string
	sourcePort     int

	astraHostname string
	astraUsername string
	astraPassword string
	astraPort     int
	listenPort    int

	debug bool
	test  bool

	p *filter.CQLProxy
)

// Method mainly to test the proxy service for now
func main() {
	parseFlags()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}


	p = &filter.CQLProxy{
		SourceHostname: sourceHostname,
		SourceUsername: sourceUsername,
		SourcePassword: sourcePassword,
		SourcePort:     sourcePort,

		AstraHostname: astraHostname,
		AstraUsername: astraUsername,
		AstraPassword: astraPassword,
		AstraPort:     astraPort,

		Port:     listenPort,
		Keyspace: "",
		MigrationPort:     15000,
	}

	// for testing purposes. to delete
	if test {
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
		case <-p.ReadyChan:
			log.Info("Coordinator received proxy ready signal.")

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

// Most of these will change to environment variables rather than flags
func parseFlags() {
	flag.StringVar(&sourceHostname, "source_hostname", "127.0.0.1", "Source Hostname")
	flag.StringVar(&sourceUsername, "source_username", "", "Source Username")
	flag.StringVar(&sourcePassword, "source_password", "", "Source Password")
	flag.IntVar(&sourcePort, "source_port", 9042, "Source Port")
	flag.StringVar(&astraHostname, "astra_hostname", "127.0.0.1", "Astra Hostname")
	flag.StringVar(&astraUsername, "astra_username", "", "Aster Username")
	flag.StringVar(&astraPassword, "astra_password", "", "Astra Password")
	flag.IntVar(&astraPort, "astra_port", 9042, "Astra Port")
	flag.IntVar(&listenPort, "listen_port", 0, "Listening Port")
	flag.BoolVar(&debug, "debug", false, "Debug Mode")
	flag.BoolVar(&test, "test", false, "Test Mode")
	flag.Parse()
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
