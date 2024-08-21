> Deployment scripts support only username and password authentication against ZDM proxy.
> Adjust credentials in _nosqlbench.yml_ file.

Usage:

1. Install [NoSQLBench](https://docs.nosqlbench.io/) and run a simple workload towards ZDM proxy. Note that
   after completing the test, pod is not terminated.

   ```
   kubectl create ns zdm-proxy
   kubectl apply -f ./nosqlbench
   ```

2. To remove all deployed artifacts, execute:

   ```
   kubectl delete -f ./nosqlbench
   ```
