package main

import (
	"cloud-gate/migration/migration"

	log "github.com/sirupsen/logrus"
)

func main() {
	conf := migration.NewConfig().ParseEnvVars()

	if conf.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	m := migration.Migration{
		Conf: conf,
	}

	err := m.Init()
	if err != nil {
		log.WithError(err).Fatal("Migration initialization failed")
	}

	go m.Migrate()

	select {}
}
