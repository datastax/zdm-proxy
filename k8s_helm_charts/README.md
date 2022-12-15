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

    ```helm -n zdmproxy install --set proxy.resources.requests.cpu=1000m --set proxy.resources.requests.memory=2000Mi --set proxy.resources.limits.cpu=1000m --set proxy.resources.limits.memory=2000Mi --set cdm.resources.requests.cpu=1000m --set cdm.resources.requests.memory=2000Mi --set cdm.resources.limits.cpu=1000m --set cdm.resources.limits.memory=2000Mi zdm-proxy ./zdm```

4. Verify that all components are up and running.

    ```kubectl -n zdmproxy get svc,cm,secret,deploy,po -o wide --show-labels```

   You can also run ```kubectl -n zdmproxy logs pod/zdm-proxy-0-xxxxxxx``` to see if there are the following entries in the log, which means everything is working as expected:

    ```
    time="2022-12-14T21:19:57Z" level=info msg="Proxy connected and ready to accept queries on 172.25.132.116:9042"
    time="2022-12-14T21:19:57Z" level=info msg="Proxy started. Waiting for SIGINT/SIGTERM to shutdown."
    ```

5. When you're done, run helm uninstall to remove all objects.

    ```helm -n zdmproxy uninstall zdm-proxy```
