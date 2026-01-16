#!/bin/sh

# Block until the given file appears or the given timeout is reached.
# Exit status is 0 iff the file exists.
wait_file() {
  local file="$1"; shift
  local wait_seconds="${1:-10}"; shift # 10 seconds as default timeout
  test $wait_seconds -lt 1 && echo 'At least 1 second is required' && return 1

  until test $((wait_seconds--)) -eq 0 -o -e "$file" ; do sleep 1; done

  test $wait_seconds -ge 0 # equivalent: let ++wait_seconds
}

donefile=/source/donefile 

wait_file "$donefile" 1200 || {
  echo "donefile missing after waiting for 1200 seconds: '$donefile'"
  exit 1
}
echo "File found"

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
  -vv

echo "Running NoSQLBench VERIFY job on TARGET"
java -jar /nb.jar \
  --show-stacktraces \
  --report-csv-to /source/verify-target \
  /source/nb-tests/cql-nb-activity.yaml \
  verify \
  driver=cqld3 \
  hosts=zdm_tests_target \
  localdc=datacenter1 \
  -vv