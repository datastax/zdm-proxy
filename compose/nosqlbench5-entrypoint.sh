#!/bin/bash

apt-get update
apt-get install -y netcat-openbsd

function test_conn() {
	nc -z -v  $1 9042;
	while [ $? -ne 0 ];
		do echo "CQL port not ready on $1";
		sleep 10;
		nc -z -v  $1 9042;
	done
}

# Wait for clusters and proxy to be responsive
test_conn zdm_tests_origin
test_conn zdm_tests_target
test_conn zdm_tests_proxy

set -e

echo "Running NoSQLBench SCHEMA job"
java -jar /nb.jar \
  --show-stacktraces \
  /source/nb-tests/cql-nb-activity.yaml \
  schema \
  driver=cqld4 \
  hosts=zdm_tests_proxy \
  localdc=datacenter1 \
  errors=retry \
  -v

echo "Running NoSQLBench RAMPUP job"
java -jar /nb.jar \
  --show-stacktraces \
  /source/nb-tests/cql-nb-activity.yaml \
  rampup \
  driver=cqld4 \
  hosts=zdm_tests_proxy \
  localdc=datacenter1 \
  errors=retry \
  -v

echo "Running NoSQLBench WRITE job"
java -jar /nb.jar \
  --show-stacktraces \
  /source/nb-tests/cql-nb-activity.yaml \
  write \
  driver=cqld4 \
  hosts=zdm_tests_proxy \
  localdc=datacenter1 \
  errors=retry \
  driverconfig='{datastax-java-driver.advanced.protocol.compression:lz4}' \
  -v

echo "Running NoSQLBench READ job"
java -jar /nb.jar \
  --show-stacktraces \
  /source/nb-tests/cql-nb-activity.yaml \
  read \
  driver=cqld4 \
  hosts=zdm_tests_proxy \
  localdc=datacenter1 \
  errors=retry \
  -v

touch /source/donefile

# don't exit otherwise the verification step on the other container won't run
sleep 600