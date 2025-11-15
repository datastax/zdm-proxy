#!/bin/sh
apk add --no-cache netcat-openbsd
apk add py3-pip
pip install cqlsh

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

echo "Creating schema"
cat /source/nb-tests/schema.cql | cqlsh zdm_tests_proxy

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

echo "Running NoSQLBench VERIFY job on ORIGIN"
java -jar /nb.jar \
  --show-stacktraces \
  --report-csv-to /source/verify-origin \
  /source/nb-tests/cql-nb-activity.yaml \
  verify \
  driver=cqld3 \
  hosts=zdm_tests_origin \
  localdc=datacenter1 \
  -v

echo "Running NoSQLBench VERIFY job on TARGET"
java -jar /nb.jar \
  --show-stacktraces \
  --report-csv-to /source/verify-target \
  /source/nb-tests/cql-nb-activity.yaml \
  verify \
  driver=cqld3 \
  hosts=zdm_tests_target \
  localdc=datacenter1 \
  -v