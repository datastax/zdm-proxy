package main

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/integration-tests/test1"

	"cloud-gate/utils"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"github.com/gocql/gocql"

	log "github.com/sirupsen/logrus"
)

func main() {
	gocql.TimeoutLimit = 5
	log.SetLevel(log.DebugLevel)

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

	// Seed source and dest with keyspace
	test.SeedKeyspace(sourceSession, destSession)

	proxyCommand := exec.Command("go", "run", "./proxy/main.go")
	proxyCommand.Env = os.Environ()
	proxyCommand.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	proxyCommand.Start()
	log.Info("PROXY STARTED")

	go test.ListenProxy()

	// Establish connection w/ proxy
	conn := test.EstablishConnection(fmt.Sprintf("127.0.0.1:14000"))

	// Run test package here
	// test1.Test1(conn, sourceSession, destSession)
	// test1.Test2(conn, sourceSession, destSession)
	test1.Test3(conn, sourceSession, destSession)
	// Kill the proxy
	pgid, err := syscall.Getpgid(proxyCommand.Process.Pid)
	if err == nil {
		syscall.Kill(-pgid, 15)
	}
	proxyCommand.Wait()

	os.Exit(0)
}
