package main

import (
	"bufio"
	"os"
	"sync"

	"cloud-gate/config"
	"cloud-gate/migration/migration"
	"cloud-gate/proxy/filter"

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

	for {
		select {
		case <-p.ReadyForRedirect:
			log.Info("Coordinate received signal that there are no more connections to Client Database.")
		}
	}
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
			}
		}
	}
}
