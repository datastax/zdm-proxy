package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"cloud-gate/migration/migration"
	"cloud-gate/proxy/pkg/config"
	"cloud-gate/proxy/pkg/filter"

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
		var tables map[string]map[string]*migration.Table
		for scanner.Scan() {
			switch scanner.Text() {
			case "start":
				tables = make(map[string]map[string]*migration.Table)
				tables["codebase"] = make(map[string]*migration.Table)
				tables["codebase"]["tasks"] = &migration.Table{
					Name:     "tasks",
					Keyspace: "codebase",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				tables["codebase"]["people"] = &migration.Table{
					Name:     "people",
					Keyspace: "codebase",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				tables["blueprint"] = make(map[string]*migration.Table)
				tables["blueprint"]["people"] = &migration.Table{
					Name:     "people",
					Keyspace: "blueprint",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				tables["mdb"] = make(map[string]*migration.Table)
				tables["mdb"]["people"] = &migration.Table{
					Name:     "people",
					Keyspace: "mdb",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				p.MigrationStart <- &migration.Status{Tables: tables,
					Lock: &sync.Mutex{}}
			case "pause":
				fmt.Println("Proxy knows to pause codebase.people. Will pause on first TRUNCATE.")
				tables["codebase"]["people"].Step = migration.UnloadingDataComplete
				tables["blueprint"]["people"].Step = migration.UnloadingDataComplete
				tables["mdb"]["people"].Step = migration.UnloadingDataComplete

			case "resume":
				fmt.Println("Resuming codebase.people")
				tables["codebase"]["people"].Step = migration.LoadingDataComplete
				p.CheckStart("codebase", "people")
				tables["blueprint"]["people"].Step = migration.LoadingDataComplete
				p.CheckStart("blueprint", "people")
				tables["mdb"]["people"].Step = migration.LoadingDataComplete
				p.CheckStart("mdb", "people")
			case "complete":
				p.MigrationDone <- struct{}{}
			case "shutdown":
				p.ShutdownChan <- struct{}{}
			}
		}
	}
}
