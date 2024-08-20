Usage:

1. Install Prometheus and Grafana in `zdm-proxy` namespace to monitor ZDM proxy instances.
   Please note that Grafana dashboards are not imported automatically.

   ```
   kubectl create ns zdm-proxy
   kubectl apply -f ./monitoring
   ```

2. If you are running on Minikube, you can access Prometheus and Grafana URLs:

   ```
   minikube service prometheus -n zdm-proxy --url
   minikube service grafana -n zdm-proxy --url
   ```

2. To remove all Prometheus and Grafana components, execute:

   ```
   kubectl delete -f ./monitoring
   ```
