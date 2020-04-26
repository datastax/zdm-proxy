package main

import (
	"cloud-gate/migration/migration"
	log "github.com/sirupsen/logrus"
)

// Method mainly to test the proxy service for now
func main() {
	conf := migration.NewConfig().ParseEnvVars()

	m := migration.Migration{
		Conf: conf,
	}

	err := m.Init()
	if err != nil {
		log.Fatal(err)
	}

	go m.Migrate()

	select {}
}
