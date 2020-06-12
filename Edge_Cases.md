# Edge Cases

## Basic Update During Unload:
1. Migration unloads data from oldDB into CSV
2. User input UPDATE command that gets applied to the oldDB. Data is not present in Astra yet, so UPDATE inserts the “updated” data.
3. Migration loads un-updated data into Astra from CSV.

Must ensure that the updates are applied.

EXAMPLE:

    # Create a source-cluster (client DB)
    # Add keyspace and populated table to client DB
    keyspace: test, table: people
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, jodie
    # Create a dest-cluster (Astra DB)
    # Add keyspace to Astra DB
    keyspace: test
    
    # Run Proxy service
    go run proxy/main.go
    
    # Unload client table to CSV
    CSV:
    id, writetime(id), name, writetime(name)
    1, 1, isabelle, 1
    2, 1, kelvin, 1
    3, 1, jodie, 1
    
    # Run UPDATE query (at Proxy port, default=14002)
    UPDATE people SET name = 'katelyn' WHERE id = 3 USING TIMESTAMP 2;
    
    # Check each table (client and Astra)
    # *Client should be updated
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, katelyn
    # *Astra should only have new insert
    PEOPLE (id, name)
    3, katelyn
    
    # Load CSV into Astra DB
    CSV: (unchanged from before)
    id, writetime(id), name, writetime(name)
    1, 1, isabelle, 1
    2, 1, kelvin, 1
    3, 1, jodie, 1
    # (Check Astra table for update)
    # *Astra matches client DB with the update
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, katelyn


## Basic Update During Load
1. Migration unloads data from oldDB to CSV.
2. Migration loads data from CSV to Astra.
3. As the migration service is loading data to Astra, an UPDATE occurs

Ensure that the update is applied to ALL the data, not just the fraction of data that has been loaded when the UPDATE occurs
* Test case follows similar structure to “update during unload” case only with different timing. This case is handled the same way as aforementioned test using the timestamps on the query/CSV.


## Update Occurs Mid-Unload
1. Migration starts unloading data from oldDB into CSV
2. Partway through the unload, one or more UPDATE commands occur. This causes the unload to contain half un-updated data and half-updated data.
3. Finish unload and load

Ensure that the updates are applied **only once** to all data in Astra.
* Test case also follows similar structure to “update during unload” case only with different timing. This case is handled the same way as aforementioned test using the timestamps on the query/CSV.


## Timestamps

Our service appends default timestamps to CQL queries that do not come with a specified default timestamp.

**Test 1**: Ensure that this appending of timestamps behaves as intended. CQLSH automatically adds custom default timestamps, but other drivers may not. 

    WITHOUT TIMESTAMP:
    [4 0 0 6 7 0 0 0 74 0 0 0 53 73 78 83 69 82 84 32 73 78 84 79 32 116 97 115 107 115 40 105 100 44 32 116 97 115 107 41 32 86 65 76 85 69 83 40 110 111 119 40 41 44 32 39 116 101 115 116 105 110 103 39 41 59 0 1 20 0 0 0 100 0 8 0 5 164 194]
    
    FLAG BYTE: I=68, value = 20
    
    (make a query object query.New() with the data given above (without timestamp)), then grab the Timestamp field from the query object, then run usingTimestamp() on that query object, and then the []byte of the query object should change into the byte array below (with the last 4 bytes being the binary.BigEndian.PutUInt32() version of the timestamp within the query object) 
    
    WITH TIMESTAMP: 
    [4 0 0 6 7 0 0 0 74 0 0 0 53 73 78 83 69 82 84 32 73 78 84 79 32 116 97 115 107 115 40 105 100 44 32 116 97 115 107 41 32 86 65 76 85 69 83 40 110 111 119 40 41 44 32 39 116 101 115 116 105 110 103 39 41 59 0 1 52 0 0 0 100 0 8 0 5 164 194  (4 TIMESTAMP BYTES HERE)] 
    
    FLAG BYTE: I=68, value = 52


## Multiple Users

For all of the above cases, we need to make sure it works with multiple users connecting to the database. For example, this means that after one user initiates an UPDATE during migration, commands from other users should be queued along with the first user’s subsequent commands.

**Keyspaces**
Test that each user can run their own `USE keyspace` commands independently and work on separate keyspaces.


## BATCH statements

**Basic Case**
If a user attempts a BATCH statement that updates a table currently under migration, ensure that all tables involved in the BATCH statement are paused. Example:

1. Migration is unloading table X.
2. BATCH statement issued with UPDATE to table X and INSERT to table Y.
3. User inputs UPDATE to table Y that would modify data inserted by the BATCH statement.
4. Migration finishes.

Ensure that the UPDATE to table Y applied to the data inserted from the BATCH statement

EXAMPLE:

    # Create a source-cluster (client DB)
    # Add keyspace and populated tables to client DB
    keyspace: test, table: people
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, jodie
    
    keyspace: test, table: places
    PLACES (state, capital)
    CA, sacremento
    TX, austin
    
    # Create a dest-cluster (Astra DB)
    # Add keyspace to Astra DB
    keyspace: test
    
    # Run Proxy service
    go run proxy/main.go
    
    # Unload table X to CSV
    CSV:
    id, writetime(id), name, writetime(name)
    1, 1, isabelle, 1
    2, 1, kelvin, 1
    3, 1, jodie, 1
    
    # BATCH statement issued with UPDATE to table X and INSERT to table Y.
    BEGIN BATCH USING TIMESTAMP 2
    
      UPDATE people SET name = 'katelyn' WHERE id = 3;
      INSERT INTO test.places(state, capital) VALUES ('MD', 'harrisburg');
    
    APPLY BATCH;
    
    # Check each table (client and Astra)
    # *Client should be updated
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, katelyn
    
    PLACES (state, capital)
    CA, sacremento
    TX, austin
    MD, harrisburg
    
    # *Astra should only have new data
    PEOPLE (id, name)
    3, katelyn
    
    PLACES (state, capital)
    MD, harrisburg
    
    # User inputs UPDATE to table Y that would modify data inserted by the BATCH statement
    UPDATE places SET capital = 'annapolis' WHERE state = 'MD';
    
    # Check each table (client and Astra)
    # *Client should be updated
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, katelyn
    
    PLACES (state, capital)
    CA, sacremento
    TX, austin
    MD, annapolis
    
    # *Astra should only have new data
    PEOPLE (id, name)
    3, katelyn
    
    PLACES (state, capital)
    MD, annapolis
    
    # Complete migration (load all tables to Astra)
    # (Check Astra table for update)
    # *Astra matches client DB with the update
    PEOPLE (id, name)
    1, isabelle
    2, kelvin
    3, katelyn
    
    PLACES (state, capital)
    CA, sacremento
    TX, austin
    MD, annapolis

**Edge Case**
Same as previous case, but if table Y has already been migrated, ensure that the proxy is able to pause table Y during migration of table X and then **restart table Y as soon as table X is done migrating**. 

