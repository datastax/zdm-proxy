#!/bin/bash

apt-get update
apt-get -y install netcat

go install github.com/go-delve/delve/cmd/dlv@latest

function test_conn() {
	nc -z -v  $1 9042;
	while [ $? -ne 0 ];
		do echo "CQL port not ready on $1";
		sleep 10;
		nc -z -v  $1 9042;
	done
}

export GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
mkdir /build
cd /build

cp /source/go.mod .
cp /source/go.sum .
cp -r /source/proxy ./proxy
cp -r /source/antlr ./antlr
ls .

# Build the application
GOOS=windows GOARCH=amd64 go build -gcflags="all=-N -l" -o main ./proxy

# Copy binary from /build to /dist
cp /build/main /main

# Wait for clusters to be ready
test_conn 192.168.100.101
test_conn 192.168.100.102

export ZDM_PROXY_LISTEN_ADDRESS="0.0.0.0"
export ZDM_METRICS_ADDRESS="0.0.0.0"
export ZDM_ORIGIN_USERNAME="foo"
export ZDM_ORIGIN_PASSWORD="foo"
export ZDM_TARGET_USERNAME="foo"
export ZDM_TARGET_PASSWORD="foo"
export ZDM_ORIGIN_CONTACT_POINTS="192.168.100.101"
export ZDM_ORIGIN_PORT="9042"
export ZDM_TARGET_CONTACT_POINTS="192.168.100.102"
export ZDM_TARGET_PORT="9042"
export ZDM_PROXY_LISTEN_PORT="9042"

# Command to run
dlv --listen=:2345 --headless=true --api-version=2 exec /main
