Prepared statements are borked

There may be an ./nb bug around non prepared update statements.

Fatal Panic on keyspace does not exist:

    insert INTO finn.user_event_history  (user_id , event_category , event_time , item_id ) VALUES ( 'seb', 'puppies', toTimestamp(now()), 'kittens' );



```
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x646e99]

goroutine 119 [running]:
cloud-gate/updates.Send.func1(0x0, 0x0, 0xc0006e60d0, 0xcb, 0xd0, 0xc0001be210, 0xc00002a540, 0xc000204000)
	/home/tato/go/src/cloud-gate/updates/updates.go:99 +0x49
created by cloud-gate/updates.Send
	/home/tato/go/src/cloud-gate/updates/updates.go:97 +0x252
```



Other Proxy Errors:

    time="2020-06-04T00:34:12Z" level=error msg="Unable to parse query: 'CALL InsightsRpc.reportInsight(?)'"

Client errors:

    16:26:50.610 [cluster1-nio-worker-1] WARN  com.datastax.driver.core.Cluster - Detected added or restarted Cassandra host localhost/127.0.0.1:14002 but ignoring it since its cluster name 'Cluster 1' does not match the one currently known (caas-cluster)


