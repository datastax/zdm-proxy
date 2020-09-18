#!/bin/bash

	OriginCassandraHostname string `required:"true" split_words:"true"`
	OriginCassandraUsername string `required:"true" split_words:"true"`
	OriginCassandraPassword string `required:"true" split_words:"true"`
	OriginCassandraPort     int    `required:"true" split_words:"true"`

	TargetCassandraHostname string `required:"true" split_words:"true"`
	TargetCassandraUsername string `required:"true" split_words:"true"`
	TargetCassandraPassword string `required:"true" split_words:"true"`
	TargetCassandraPort     int    `required:"true" split_words:"true"`

	ProxyServiceHostname       string `required:"true" split_words:"true"`
	ProxyCommunicationPort     int    `required:"true" split_words:"true"`
	ProxyMetricsPort           int    `required:"true" split_words:"true"`
	ProxyQueryPort             int    `split_words:"true"`

	MaxQueueSize int `default:"1000" split_words:"true"`

	Test  bool
	Debug bool


export ORIGIN_CASSANDRA_HOSTNAME="34.208.223.197"
export ORIGIN_CASSANDRA_USERNAME="cassandra"
export ORIGIN_CASSANDRA_PASSWORD="cassandra"
export ORIGIN_CASSANDRA_PORT=9042

export TARGET_CASSANDRA_HOSTNAME="caas-cluster-caas-dc-service"
export TARGET_CASSANDRA_USERNAME="seb_hacking"
export TARGET_CASSANDRA_PASSWORD="seb_hacking"
export TARGET_CASSANDRA_PORT=9042

export PROXY_SERVICE_HOSTNAME="127.0.0.1"
export PROXY_COMMUNICATION_PORT=14000
export PROXY_METRICS_PORT=14001
export PROXY_QUERY_PORT=14002

export DEBUG=true
export TEST=true

## change accordingly
ls /opt/cloud-gate/bin/

#nohup /opt/cloud-gate/bin/proxy &
echo start | /opt/cloud-gate/bin/proxy start
