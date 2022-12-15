Usage:
1. Create dedicated namespace for zdmproxy.

    ```kubectl create ns zdmproxy```

2. Import Secure Connect Bundle into a k8s secret called zdmproxy-scb.

    ```kubectl -n zdmproxy create secret generic zdmproxy-scb --from-file=/tmp/secure-connect-origin.zip --from-file=/tmp/secure-connect-target.zip```

3. Run helm install to deploy the helm charts.

    ```helm -n zdmproxy install --set origin_username="$prod_astra_user" --set origin_password="$prod_astra_escaped_password" --set target_username="$prod_astra_user" --set target_password="$prod_astra_escaped_password" zdm-proxy ./zdm```

4. Verify that all components are up and running.

    ```kubectl -n zdmproxy get svc,cm,secret,deploy,po -o wide --show-labels```

   You can also run ```kubectl -n zdmproxy logs pod/zdm-proxy-0-xxxxxxx``` to see if there are the following entries in the log, which means everything is working as expected:

    ```
    time="2022-12-14T21:19:57Z" level=info msg="Proxy connected and ready to accept queries on 172.25.132.116:9042"
    time="2022-12-14T21:19:57Z" level=info msg="Proxy started. Waiting for SIGINT/SIGTERM to shutdown."
    ```

5. When you're done, run helm uninstall to remove all objects.

    ```helm uninstall zdm-proxy```
