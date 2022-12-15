Usage:
1. Create dedicated namespace for zdmproxy.

    ```
    kubectl create ns zdmproxy
    ```

2. Import Secure Connect Bundle into a k8s secret called `zdmproxy-scb`. Note the secret file name must be `secure-connect-target.zip`, so you may need to rename the `.zip` file downloaded from Astra:

   ```
   kubectl -n zdmproxy create secret generic zdmproxy-scb --from-file=/tmp/secure-connect-target.zip
   ```

3. Import origin and target connection details into another secret called `zdmproxy`. 

   1. Set the following environment varibles as appropriate, substituting the values of course. To prevent exposing passwords on .bash_history, create a temporary file `my.env`:
   ```
   origin_contact_points="node1.example.com,node2.example.com,node3.example.com"
   origin_port="9042"
   origin_username="cassandra"
   origin_password="cassandra"
   target_username="xgMZgHTYIoyPcpjphJQkdq"
   target_password="QbfFdO+,JipCUbdGrL01LRX0c9SyW4RYF9uPPtnvZ4upqIYX7fgbDw7p8RYvL-J_sBFjQZ7yfvWquxrQnCjZjWm,Q2BxQq,dMvT8QuInWla8FGAi5,AKzkIdE1D2Lsse"
   ```

   2. Set these into your environment: 
   ```
   . ./my.env
   ```

   3. Using these environment variables, create another k8s secret called zdmproxy:

    ```
    kubectl -n zdmproxy create secret generic zdmproxy \
      --from-literal=origin_contact_points="$origin_contact_points" \
      --from-literal=origin_port="$origin_port" \
      --from-literal=origin_username="$origin_username" \
      --from-literal=origin_password="$origin_password" \
      --from-literal=target_username="$target_username" \
      --from-literal=target_password="$target_password"
    ```

   4. Remove this temporary file:  
   ```
   rm ./my.env
   ```


4. Run helm install to deploy the helm charts; the default `cpu` and `memory` resource allocations are designed for production environment:

    ```
    helm -n zdmproxy install zdm-proxy ./zdm
    ```

   If you see pods pending due to not enough resources, try to use the following commands instead:

    ```
    helm -n zdmproxy uninstall zdm-proxy
    ```

    ```
    helm -n zdmproxy install \
        --set proxy.resources.requests.cpu=1000m \
        --set proxy.resources.requests.memory=2000Mi \
        --set proxy.resources.limits.cpu=1000m \
        --set proxy.resources.limits.memory=2000Mi \
        --set cdm.resources.requests.cpu=1000m \
        --set cdm.resources.requests.memory=2000Mi \
        --set cdm.resources.limits.cpu=1000m \
        --set cdm.resources.limits.memory=2000Mi \
      zdm-proxy ./zdm
    ```

5. Verify that all components are up and running.

    ```
    kubectl -n zdmproxy get svc,cm,secret,deploy,po -o wide --show-labels
    ```

   You can also run ```kubectl -n zdmproxy logs pod/zdm-proxy-0-xxxxxxx``` to see if there are the following entries in the log, which means everything is working as expected:

    ```
    time="2022-12-14T21:19:57Z" level=info msg="Proxy connected and ready to accept queries on 172.25.132.116:9042"
    time="2022-12-14T21:19:57Z" level=info msg="Proxy started. Waiting for SIGINT/SIGTERM to shutdown."
    ```

6. When you're done, delete the namespace to remove all objects:

    ```
    kubectl delete namespace zdmproxy
    ```
