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
	cqlServer := client.NewCqlServer(addr, &client.AuthCredentials{
		Username: username,
		Password: password,
	})
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
	ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
	return recv.CqlServer.Start(ctx)
}

func (recv *Cluster) Close() error {
	return recv.CqlServer.Close()
}