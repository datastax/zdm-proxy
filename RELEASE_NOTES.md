# Release Notes

General release notes for the ZDM Proxy.

Build artifacts are available at [Docker Hub](https://hub.docker.com/repository/docker/datastax/zdm-proxy).

For additional details on the changes included in a specific release, see the associated CHANGELOG-x.x.md file.

## v2.3.2 - 2025-04-14

Ignore forwarding CQL requests for DSE Insights Client to target cluster

Upgrade software dependencies to resolve vulnerabilities:
- GoLang to 1.24.2

## v2.3.1 - 2024-11-08

Upgrade software dependencies to resolve vulnerabilities:
- GoLang to 1.22
- _github.com/prometheus/client_golang_ to 1.11.1
- _google.golang.org/protobuf_ to 1.33.0

[Changelog](CHANGELOG/CHANGELOG-2.3.md#v231---2024-11-08)

## v2.3.0 - 2024-07-04

Support providing configuration of ZDM with YAML file.

[Changelog](CHANGELOG/CHANGELOG-2.3.md#v230---2024-07-04)

## v2.2.0 - 2024-06-11

Support SAI queries and third-party authenticators.

[Changelog](CHANGELOG/CHANGELOG-2.2.md#v220---2024-06-11)

## v2.1.0 - 2023-11-13

Enables read only workloads to work correctly with ZDM-Proxy and Datastax Astra. 

Astra terminates idle connections after a period of inactivity and Datastax Drivers send periodic heartbeats by default if the user does not send any request in a time interval so this is not an issue in a normal deployment. 

With ZDM-Proxy in the mix, there won't be an issue if the workload has both reads and writes because if a heartbeat is sent by the client application then the proxy will forward it to both Origin and Target. However, if the client application can have time periods where only read requests are sent then the client application will not send a heartbeat (because requests are being sent through the connection) but the SECONDARY cluster will not receive any requests during that time period (reads are sent to PRIMARY only) which can cause a disconnect due to an idle connection.

This release enables the ZDM-Proxy to independently send heartbeats on the SECONDARY cluster connection if that connection has not seen any activity over a certain period of time which should fix the issue of disconnects happening when a client application was only sending read requests to the proxy.

[Changelog](CHANGELOG/CHANGELOG-2.1.md#v210---2023-11-13)

## v2.0.0 - 2022-10-18

Open-source release that also includes renaming of the product, public documentation launch and a revamp of configuration and metrics.

[Changelog](CHANGELOG/CHANGELOG-2.0.md#v200---2022-10-18)