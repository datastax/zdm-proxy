package main

import (
	"bufio"
	"cloud-gate/proxy"
	"flag"
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
)

// Method mainly to test the proxy service for now
func main() {
	parseFlags()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	// Channel for migrator to communicate with proxy when the migration process has begun
	migrationStartChan := make(chan *proxy.MigrationStatus, 1)

	// Channel for migration service to send a signal through, directing the proxy to forward all traffic directly
	// to the Astra DB
	migrationCompleteChan := make(chan struct{})

	// Channel for the migration service to send us Table structs for tables that have completed migration
	tableMigratedChan := make(chan *proxy.Table, 1)

	p := proxy.CQLProxy{
		SourceHostname: sourceHostname,
		SourceUsername: sourceUsername,
		SourcePassword: sourcePassword,
		SourcePort:     sourcePort,

		AstraHostname: astraHostname,
		AstraUsername: astraUsername,
		AstraPassword: astraPassword,
		AstraPort:     astraPort,

		Port: listenPort,

		MigrationStartChan:    migrationStartChan,
		MigrationCompleteChan: migrationCompleteChan,
		TableMigratedChan:  tableMigratedChan,
	}

	// for testing purposes. to delete
	if test {
		go doTesting(&p)
	}

	err := p.Start()
	if err != nil {
		// TODO: handle error
		panic(err)
	}

	for {
		select {
		case <-p.ReadyChan:
			log.Info("Coordinator received proxy ready signal.")
			err := p.Listen()
			if err != nil {
				panic(err)
			}
		case <-p.ReadyForRedirect:
			log.Info("Coordinate received signal that there are no more connections to Client Database.")
		}
	}
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
func doTesting(p *proxy.CQLProxy) {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "start":
				tables := make(map[string]proxy.Table)
				tables["tasks"] = proxy.Table{
					Name:     "tasks",
					Keyspace: "codebase",
					Status:   proxy.WAITING,
					Error:    nil,
				}

				p.MigrationStartChan <- &proxy.MigrationStatus{Tables: tables,
					Lock: sync.Mutex{}}
			case "complete":
				p.MigrationCompleteChan <- struct{}{}
			case "shutdown":
				p.ShutdownChan <- struct{}{}
			}
		}
	}
}
