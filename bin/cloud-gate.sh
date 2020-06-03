#!/bin/bash

export SOURCE_HOSTNAME="54.184.225.49"
export SOURCE_USERNAME="cassandra"
export SOURCE_PASSWORD="cassandra"
export SOURCE_PORT=9042
export ASTRA_HOSTNAME="caas-cluster-caas-dc-service"
export ASTRA_USERNAME="cassandra"
export ASTRA_PASSWORD="cassandra"
export ASTRA_PORT=9042
export MIGRATION_SERVICE_HOSTNAME="127.0.0.1"
export MIGRATION_COMMUNICATION_PORT=15000
export PROXY_SERVICE_HOSTNAME="127.0.0.1"
export PROXY_COMMUNICATION_PORT=14000
export MIGRATION_ID="test"
export MIGRATION_S3="cloud-gate-test"
export MIGRATION_COMPLETE=false
#export DEBUG=true
export TEST=true
#export TEST=false
export PROXY_METRICS_PORT=14001
export PROXY_QUERY_PORT=14002
export DSBULK_PATH=~/Downloads/dsbulk-1.5.0/bin/dsbulk

ls /opt/cloud-gate/bin/

#nohup /opt/cloud-gate/bin/proxy &
#/opt/cloud-gate/bin/mig
echo start | /opt/cloud-gate/bin/proxy start
