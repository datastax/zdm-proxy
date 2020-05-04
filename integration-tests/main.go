package main

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/integration-tests/test1"
	"cloud-gate/utils"
	"fmt"
	"os"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	// Initialize test data
	test.DataIds = []string{
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
		"eed574b0-8c20-11ea-9fc6-6d2c86545d91"}
	test.DataTasks = []string{
		"MSzZMTWA9hw6tkYWPTxT0XfGL9nGQUpy",
		"IH0FC3aWM4ynriOFvtr5TfiKxziR5aB1",
		"FgQfJesbNcxAebzFPRRcW2p1bBtoz1P1"}

	// Connect to source and dest sessions
	var err error
	sourceSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9042)
	if err != nil {
		log.WithError(err).Error("Error connecting to source cluster.")
	}
	defer sourceSession.Close()

	destSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9043)
	if err != nil {
		log.WithError(err).Error("Error connecting to dest cluster.")
	}
	defer destSession.Close()

	// Drop all existing data
	test.DropExistingKeyspace(sourceSession, destSession)

	// Seed source and dest with keyspace
	test.SeedKeyspace(sourceSession, destSession)

	// Seed source and dest w/ schema and data
	test.SeedData(sourceSession, destSession)

	proxyCommand := exec.Command("go", "run", "./proxy/main.go")
	proxyCommand.Env = os.Environ()

	// proxyOut, _ := proxyCommand.StdoutPipe()
	// go test.PrintProxyOutput(proxyOut)
	go test.ListenProxy()

	proxyCommand.Start()

	log.Info("PROXY STARTED")

	log.Info("Sleeping...")
	time.Sleep(time.Second * 10)

	// Establish connection w/ proxy
	conn := test.EstablishConnection(fmt.Sprintf("127.0.0.1:14000"))

	// Run test package here
	test1.Test1(conn, sourceSession, destSession)
}
