package utils

import (
	"fmt"
	"github.com/gocql/gocql"
)

func ConnectToCluster(hostname string, username string, password string, port int) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hostname)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Port = port

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	fmt.Printf("Connection established with Cluster (%s:%d, %s, %s)",
		hostname, port, username, password)

	return session, nil
}

