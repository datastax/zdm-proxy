package main

import (
	"cloud-gate/migration"
	"flag"

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

	// Channel for migrator to communicate with proxy when the migration process has begun
	migrationStartChan := make(chan *migration.Status, 1)

	// Channel for migration service to send a signal through, directing the proxy to forward all traffic directly
	// to the Astra DB
	migrationCompleteChan := make(chan struct{})

	m := migration.Migration{
		Keyspace:    keyspace,
		DsbulkPath:  dsbulkPath,
		HardRestart: hardRestart,

		SourceHostname: sourceHostname,
		SourceUsername: sourceUsername,
		SourcePassword: sourcePassword,
		SourcePort:     sourcePort,

		DestHostname: astraHostname,
		DestUsername: astraUsername,
		DestPassword: astraPassword,
		DestPort:     astraPort,

		MigrationStartChan: migrationStartChan,
		MigrationCompleteChan: migrationCompleteChan,
	}

	err := m.Init()
	if err != nil {
		log.Fatal(err)
	}

	go m.Migrate()

	for {
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
