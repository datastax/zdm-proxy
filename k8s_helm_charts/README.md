Usage:
1. Create dedicated namespace for zdmproxy.

    ```kubectl create ns zdmproxy```

2. Import Secure Connect Bundle (make sure the filename is exactly secure-connect-target.zip) into a k8s secret called zdmproxy-scb.

    ```kubectl -n zdmproxy create secret generic zdmproxy-scb --from-file=/tmp/secure-connect-target.zip```

   Import Origin contact points, Origin port, Origin username and password, Target/Astra client ID and client Secret into another k8s secret called zdmproxy.

    ```kubectl -n zdmproxy create secret generic zdmproxy --from-literal=origin_contact_points="$origin_contact_points" --from-literal=origin_port="$origin_port" --from-literal=origin_username="$origin_username" --from-literal=origin_password="$origin_password" --from-literal=target_username="$target_username" --from-literal=target_password="$target_password"```

3. Run helm install to deploy the helm charts.

    ```helm -n zdmproxy install zdm-proxy ./zdm```

   The default resource allocations _(memory and cpu) are designed for production environment, if you see pods pending due to not enough resources, try to use the following commands instead:

    ```helm -n zdmproxy uninstall zdm-proxy```

    ```helm -n zdmproxy install --set resources.requests.cpu=1000m --set resources.requests.memory=2000Mi --set resources.limits.cpu=1000m --set resources.limits.memory=2000Mi zdm-proxy ./zdm```

4. Verify that all components are up and running.

    ```kubectl -n zdmproxy get svc,ep,po,secret -o wide --show-labels```

   You can also run ```kubectl -n zdmproxy logs pod/zdm-proxy-0``` to see if there are the following entries in the log, which means everything is working as expected:


    ```
    time="2022-12-14T21:19:57Z" level=info msg="Proxy connected and ready to accept queries on 172.25.132.116:9042"
    time="2022-12-14T21:19:57Z" level=info msg="Proxy started. Waiting for SIGINT/SIGTERM to shutdown."
    ```

5. Switch primary cluster to target (all proxy pods will automatically roll-restart after the change).

    ```helm -n zdmproxy upgrade zdm-proxy ./zdm --set primaryCluster=TARGET```

6. Scale out/in to different number of proxy pods.

    ```helm -n zdmproxy upgrade zdm-proxy ./zdm --set count=5```

    Note: if you've already switched primary cluster to target, make sure you add ```--set primaryCluster=TARGET``` in this command line as well. An alternative is to directly edit zdm/values.yaml then run helm upgrade.

7. When you're done, run helm uninstall to remove all objects.


    ```helm -n zdmproxy uninstall zdm-proxy```
