#!/bin/bash

# scale down studio
# kubectl scale statefulsets studio --replicas 0  --namespace aeceb92e-04b4-40a0-9ee0-6633e45b2a53

docker build -t cloud-gate ./
docker tag cloud-gate gcr.io/astrae2dv4yertwzx3d4hdbr6llxm/cloud-gate:latest
docker push gcr.io/astrae2dv4yertwzx3d4hdbr6llxm/cloud-gate:latest

kubectl delete deployment cloud-gate --namespace 8c1935a9-d5ae-45a3-9ec8-604e446549b1
kubectl apply -f cloud-gate.yaml  --namespace 8c1935a9-d5ae-45a3-9ec8-604e446549b1
