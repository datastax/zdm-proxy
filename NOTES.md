
Proxy Errors:

    time="2020-06-04T00:34:12Z" level=error msg="Unable to parse query: 'CALL InsightsRpc.reportInsight(?)'"


Client errors:

    16:26:50.610 [cluster1-nio-worker-1] WARN  com.datastax.driver.core.Cluster - Detected added or restarted Cassandra host localhost/127.0.0.1:14002 but ignoring it since its cluster name 'Cluster 1' does not match the one currently known (caas-cluster)


Prepared statements seem borked

There may be an ./nb bug around non prepared update statements.
