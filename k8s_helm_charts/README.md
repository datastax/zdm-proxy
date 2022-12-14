Usage:
1. Create dedicated namespace for zdmproxy.

    ```kubectl create ns zdmproxy```

2. Import Secure Connect Bundle into a k8s secret called zdmproxy-scb.

    ```kubectl -n zdmproxy create secret generic zdmproxy-scb --from-file=/tmp/secure-connect-origin.zip --from-file=/tmp/secure-connect-target.zip```

3. Run helm install to deploy the helm charts.

    ```helm -n zdmproxy install --set origin_username="$prod_astra_user" --set origin_password="$prod_astra_escaped_password" --set target_username="$prod_astra_user" --set target_password="$prod_astra_escaped_password" zdm-proxy ./zdm```

4. Verify that all components are up and running.

    ```kubectl -n zdmproxy get svc,cm,secret,deploy,po -o wide --show-labels```

5. When you're done, run helm uninstall to remove all objects.

    ```helm uninstall zdm-proxy```
