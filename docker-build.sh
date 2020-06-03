#!/bin/bash

# scale down studio
# kubectl scale statefulsets studio --replicas 0  --namespace aeceb92e-04b4-40a0-9ee0-6633e45b2a53

docker build -t cloud-gate ./
docker tag cloud-gate gcr.io/astraqvoiippbnugrc3no46vmon4f/cloud-gate:latest
docker push gcr.io/astraqvoiippbnugrc3no46vmon4f/cloud-gate:latest

# kubectl delete deployment cloud-gate --namespace aeceb92e-04b4-40a0-9ee0-6633e45b2a53
kubectl apply -f cloud-gate.yaml  --namespace aeceb92e-04b4-40a0-9ee0-6633e45b2a53
