DROP KEYSPACE IF EXISTS test;

create keyspace if not exists test
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    AND durable_writes = true;

create table if not exists test.keyvalue (
    key int,
    value text,
    PRIMARY KEY (key));