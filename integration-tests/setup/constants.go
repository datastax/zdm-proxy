package setup

// TestKeyspace is the dedicated keyspace for testing
const TestKeyspace = "cloudgate_test"

// TestTable is the dedicated table for testing
const TestTable = "tasks"

// TestTable2 is another dedicated table for testing
const TestTable2 = "people"

// TestTables is an array of dedicated tables for testing
var TestTables = [...]string{TestTable, TestTable2}
