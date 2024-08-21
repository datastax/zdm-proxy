Usage:

1. Create dedicated namespace for ZDM Proxy.

    ```kubectl create ns zdm-proxy```

2. Update `values.yaml` file to reflect configuration of your ZDM proxy environment. All files should be placed
   in the same directory as values file. Helm chart will automatically create Kubernetes secrets for passwords,
   TLS certificates and Secure Connection Bundle.

3. Install Helm chart in desired Kubernetes namespace.

    ```helm -n zdm-proxy install zdm-proxy zdm```

   The default resource allocations (memory and CPU) are designed for production environment,
   if you see PODs pending due to not enough resources, try to use the following commands instead:

   ```
   helm -n zdm-proxy install --set resources.requests.cpu=1000m --set resources.requests.memory=2000Mi \
        --set resources.limits.cpu=1000m --set resources.limits.memory=2000Mi zdm-proxy zdm
   ```

4. Verify that all components are up and running.

   ```kubectl -n zdm-proxy get svc,ep,po,secret -o wide --show-labels```

   You can also run `kubectl -n zdm-proxy logs pod/zdm-proxy-0` to see if there are the following entries in the log,
   which means ZDM Proxy is working as expected:

    ```
    time="2022-12-14T21:19:57Z" level=info msg="Proxy connected and ready to accept queries on 172.25.132.116:9042"
    time="2022-12-14T21:19:57Z" level=info msg="Proxy started. Waiting for SIGINT/SIGTERM to shutdown."
    ```

5. Optionally you can install monitoring components defined in `monitoring` subfolder.

6. To generate example load, you can use [NoSQLBench](https://docs.nosqlbench.io/) tool. Check out deployment scripts in `nosqlbench` subfolder.

7. Basic ZDM Proxy operations.

   - Switch primary cluster to target (all proxy pods will automatically roll-restart after the change).

      ```helm -n zdm-proxy upgrade zdm-proxy zdm --set primaryCluster=TARGET```

   - Scale to different number of proxy pods.

      ```helm -n zdm-proxy upgrade zdm-proxy ./zdm --set count=5```

      Note: if you've already switched primary cluster to target, make sure you add `--set primaryCluster=TARGET`
      in this command line as well. An alternative is to directly edit `zdm/values.yaml` then run Helm upgrade.

8. When you're done, run helm uninstall to remove all objects.

    ```helm -n zdmproxy uninstall zdm-proxy```

![Demo](zdm-k8s-ccm-astra.gif)