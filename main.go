package main

import (
	"bufio"
	"cloud-gate/migration"
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

	keyspace    string
	dsbulkPath  string
	hardRestart bool
)

// Method mainly to test the proxy service for now
func main() {
	parseFlags()

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
		TableMigratedChan:     tableMigratedChan,
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
	flag.StringVar(&keyspace, "k", "", "Keyspace to migrate")
	flag.StringVar(&dsbulkPath, "d", "/Users/terranceli/Documents/projects/codebase/datastax-s20/dsbulk-1.4.1/bin/dsbulk", "dsbulk executable path")
	flag.BoolVar(&hardRestart, "r", false, "Hard restart (ignore checkpoint)")
	flag.Parse()
}
