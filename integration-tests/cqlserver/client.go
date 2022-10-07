package cqlserver

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type Client struct {
	CqlClient     *client.CqlClient
	CqlConnection *client.CqlClientConnection
}

func NewCqlClient(addr string, port int, username string, password string, connect bool) (*Client, error) {
	var authCreds *client.AuthCredentials
	if username != "" || password != "" {
		authCreds = &client.AuthCredentials{
			Username: username,
			Password: password,
		}
	}
	proxyAddr := fmt.Sprintf("%s:%d", addr, port)
	clt := client.NewCqlClient(proxyAddr, authCreds)

	var clientConn *client.CqlClientConnection
	var err error
	if connect {
		clientConn, err = clt.Connect(context.Background())
		if err != nil {
			return nil, err
		}
	}

	return &Client{
		CqlClient:     clt,
		CqlConnection: clientConn,
	}, nil
}

func (recv *Client) Connect(version primitive.ProtocolVersion) error {
	clientConn, err := recv.CqlClient.ConnectAndInit(context.Background(), version, client.ManagedStreamId)
	if err != nil {
		return err
	}
	recv.CqlConnection = clientConn
	return nil
}

func (recv *Client) Close() error {
	if recv.CqlConnection != nil {
		return recv.CqlConnection.Close()
	}

	return nil
}
