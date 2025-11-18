#!/bin/sh

wget https://github.com/nosqlbench/nosqlbench/releases/download/nosqlbench-4.15.104/nb.jar

mv nb.jar /

set -e

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