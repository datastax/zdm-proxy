package cqlserver

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	log "github.com/sirupsen/logrus"
	"time"
)

type Cluster struct {
	InitialContactPoint string
	CqlServer           *client.CqlServer
}

func NewCqlServerCluster(listenAddr string, port int, username string, password string, start bool) (*Cluster, error) {
	addr := fmt.Sprintf("%s:%d", listenAddr, port)
	var authCreds *client.AuthCredentials
	if username != "" || password != "" {
		authCreds = &client.AuthCredentials{
			Username: username,
			Password: password,
		}
	}
	cqlServer := client.NewCqlServer(addr, authCreds)
	cqlServer.RequestHandlers = []client.RequestHandler{
		client.NewDriverConnectionInitializationHandler("test_cluster", "dc1", func(_ string) {}),
	}
	if start {
		err := cqlServer.Start(context.Background())
		if err != nil {
			err2 := cqlServer.Close()
			if err2 != nil {
				log.Warnf("error closing cql server after start failed: %v", err2)
			}
			return nil, err
		}
	}
	return &Cluster{
		InitialContactPoint: addr,
		CqlServer:           cqlServer,
	}, nil
}

func (recv *Cluster) Start() error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return recv.CqlServer.Start(ctx)
}

func (recv *Cluster) Close() error {
	return recv.CqlServer.Close()
}
