package client

import (
	"bytes"
	"errors"
)

type Authenticator interface {
	InitialResponse(name string) ([]byte, error)
	EvaluateChallenge(challenge []byte) ([]byte, error)
}

type DseAuthenticator interface {
	Authenticator
	GetInitialServerChallenge() []byte
	GetMechanism() []byte
}

type DsePlainTextAuthenticator struct {
	username string
	password string
}

var (
	initialServerChallenge = []byte("PLAIN-START")
	mechanism              = []byte("PLAIN")
)

func NewDsePlainTextAuthenticator(username string, password string) *DsePlainTextAuthenticator {
	return &DsePlainTextAuthenticator{
		username: username,
		password: password,
	}
}

func (dsePlainTextAuth *DsePlainTextAuthenticator) InitialResponse(name string) ([]byte, error) {
	return initialResponse(dsePlainTextAuth, name)
}

func (dsePlainTextAuth *DsePlainTextAuthenticator) EvaluateChallenge(challenge []byte) ([]byte, error) {
	if challenge == nil || bytes.Compare(challenge, initialServerChallenge) != 0 {
		return nil, errors.New("incorrect SASL challenge from server")
	}

	buffer := make([]byte, len(dsePlainTextAuth.username)+len(dsePlainTextAuth.password)+2)
	buffer[0] = 0
	buffer = append(buffer[0:1], []byte(dsePlainTextAuth.username)...)
	buffer = append(buffer, 0)
	buffer = append(buffer, []byte(dsePlainTextAuth.password)...)
	return buffer, nil
}

func (dsePlainTextAuth *DsePlainTextAuthenticator) GetInitialServerChallenge() []byte {
	return initialServerChallenge
}

func (dsePlainTextAuth *DsePlainTextAuthenticator) GetMechanism() []byte {
	return mechanism
}

func initialResponse(auth DseAuthenticator, name string) ([]byte, error) {
	if !isDseAuthenticator(name) {
		return auth.EvaluateChallenge(auth.GetInitialServerChallenge())
	}

	return auth.GetMechanism(), nil
}

func isDseAuthenticator(name string) bool {
	return name == "com.datastax.bdp.cassandra.auth.DseAuthenticator"
}
