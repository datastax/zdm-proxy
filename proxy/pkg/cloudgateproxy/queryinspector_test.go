package cloudgateproxy

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInspectCqlQuery(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		statementType statementType
		keyspaceName  string
		tableName     string
	}{
		// SELECT statements
		{
			"simple SELECT",
			"SELECT foo, bar, qix FROM table1 WHERE foo = 1;",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"qualified SELECT",
			"SELECT foo, bar, qix FROM ks1.table1 WHERE foo = 1;",
			statementTypeSelect,
			"ks1",
			"table1",
		},
		{
			"simple SELECT star",
			"SELECT * FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"qualified SELECT star",
			"SELECT * FROM ks1.TABLE1",
			statementTypeSelect,
			"ks1",
			"table1",
		},
		{
			"SELECT COUNT star",
			"SELECT COUNT ( * ) FROM ks1.TABLE1",
			statementTypeSelect,
			"ks1",
			"table1",
		},
		{
			"quoted SELECT",
			"SELECT foo, bar, qix FROM \"MyTable\"",
			statementTypeSelect,
			"",
			"MyTable",
		},
		{
			"quoted qualified SELECT",
			"SELECT foo, bar, qix FROM \"MyKeyspace\" . \"MyTable\"",
			statementTypeSelect,
			"MyKeyspace",
			"MyTable",
		},
		{
			"quoted qualified SELECT with quotes",
			"SELECT foo, bar, qix FROM \"MyKeyspace\" . \"My\"\"Table\"",
			statementTypeSelect,
			"MyKeyspace",
			"My\"Table",
		},
		{
			"unreserved keywords",
			"SELECT * FROM FILTERING.TINYINT WHERE foo = 1;",
			statementTypeSelect,
			"filtering",
			"tinyint",
		},
		{
			"complex SELECT",
			"SELECT foo, " +
				"\"BAR\" AS bar, " +
				"'literal', " +
				"$$ plsql-style literal $$, " +
				"-NaN, 0.1, true, PT2S, 0xcafebabe, " +
				"97bda55b-6175-4c39-9e04-7c0205c709dc, " +
				"system.now(), " +
				"\"system\".myFunction( col1, 2, 0.1, false, 'foo', \"system\".cos(3.14) ), " +
				"CAST(qix AS varchar), " +
				"(list<varchar>) [ 'a', 'b' ] " +
				"FROM ks1 . table1 " +
				"WHERE foo = 0.1 " +
				"AND token ( pk1, pk2 ) >= -9876543321" +
				"AND \"MyCol\" LIKE 'search' " +
				"AND col2 IS NOT NULL " +
				"AND \"MyMap\" CONTAINS key 'foo' " +
				"AND \"MyMap\" [ 'key' ] = PT2S " +
				"AND col IN ( )  " +
				"AND col IN ( :col1, :col2 )  " +
				"AND col IN :bindMarker  " +
				"AND ( \"MyCol1\" , \"MyCol2\" ) IN () " +
				"AND ( \"MyCol1\" , \"MyCol2\" ) IN ( ( true , false) , ( 0.1, 2.3 ) ) " +
				"AND ( \"MyCol1\" , \"MyCol2\" ) IN (?,?) " +
				"AND ( \"MyCol1\" , \"MyCol2\" ) >= (1, 2, 3) " +
				"AND ( \"MyCol1\" , \"MyCol2\" ) < ?",
			statementTypeSelect,
			"ks1",
			"table1",
		},
		{
			"json SELECT",
			"SELECT JSON DISTINCT foo, bar, qix FROM table1 WHERE foo = 1;",
			statementTypeSelect,
			"",
			"table1",
		},
		// whitespace and comments before SELECT statement
		{
			"whitespace before SELECT",
			"   \t\r\n   SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"single line comment dash",
			"-- blah  \n   SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"single line comment dash Windows",
			"-- blah  \r\n   SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"single line comment slash",
			"// blah  \n   SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"single line comment slash Windows",
			"// blah  \r\n   SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"multi line comment 1 line",
			"/* blah */  SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"multi line comment 2 lines",
			"/* blah  \t\r\n */  SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		{
			"many comments",
			"-- comment1 \n // comment 2 \n /* comment 2\t\r\n */  " +
				"SELECT foo, bar FROM table1",
			statementTypeSelect,
			"",
			"table1",
		},
		// USE
		{
			"simple USE",
			"USE ks1",
			statementTypeUse,
			"ks1",
			"",
		},
		// INSERT
		{
			"simple INSERT",
			"INSERT INTO ks1.table1 (foo, bar) VALUES (1, now())",
			statementTypeInsert,
			"ks1",
			"table1",
		},
		{
			"complex INSERT",
			"INSERT INTO \"MyKeyspace\" . \"MyTable\" " +
				// test a few unreserved keywords
				"( foo, \"BAR\", tinyint, cast, json, filtering) " +
				"VALUES ('literal', $$ plsql-style literal $$, -NaN, 0.1, true, PT2S, 0xcafebabe, 97bda55b-6175-4c39-9e04-7c0205c709dc, system.now(), (list<varchar>) [ 'a', 'b' ]) " +
				"if not exists USING timestamp 1234 AND ttl 123",
			statementTypeInsert,
			"MyKeyspace",
			"MyTable",
		},
		// UPDATE
		{
			"simple UPDATE",
			"UPDATE ks1.table1 SET foo = 1, bar = 2 WHERE qix = 42",
			statementTypeUpdate,
			"ks1",
			"table1",
		},
		{
			"complex UPDATE",
			"UPDATE \"MyKeyspace\" . \"MyTable\" " +
				"USING TIMESTAMP 1234 and ttl 123 " +
				"SET foo = 1, bar = system.now(), foo = 'literal', " +
				"\"BAR\" = $$ plsql-style literal $$, " +
				// test a few unreserved keywords
				"tinyint = -NaN, cast = 0.1, writetime = true, json = PT2S, filtering = 0xcafebabe " +
				"WHERE foo = \"MyKeyspace\".whatever(123) " +
				"AND bar = (list<varchar>) [ 'a', 'b' ] " +
				"IF qix IN (97bda55b-6175-4c39-9e04-7c0205c709dc)",
			statementTypeUpdate,
			"MyKeyspace",
			"MyTable",
		},
		// DELETE
		{
			"simple DELETE",
			"DELETE FROM ks1.table1 WHERE qix = 123",
			statementTypeDelete,
			"ks1",
			"table1",
		},
		{
			"complex DELETE",
			"DELETE foo, bar FROM \"MyKeyspace\" . \"MyTable\" " +
				"USING TIMESTAMP ? " +
				"WHERE foo = 123 AND bar = 0xcafebabe AND (c1, c2, c3) IN ((1,2,3),(2,3,4)) " +
				"IF EXISTS",
			statementTypeDelete,
			"MyKeyspace",
			"MyTable",
		},
		// BATCH
		{
			"simple BATCH",
			"BEGIN UNLOGGED BATCH " +
				"INSERT INTO ks1.table1 (foo, bar) VALUES (1, now()); " +
				"UPDATE ks1.table2 USING TIMESTAMP 1234 SET foo = 1, bar = now() WHERE bar = 42 " +
				"DELETE foo, bar FROM ks1.table3 USING TIMESTAMP 1234 WHERE qix = 123 " +
				"APPLY BATCH",
			statementTypeBatch,
			"ks1",
			"table3",
		},
		// UNRECOGNIZED
		{
			"INSERT JSON",
			"INSERT INTO table1 JSON '{}'",
			statementTypeOther,
			"",
			"",
		},
		{
			"simple CREATE",
			"CREATE TABLE ks1.table1 blah",
			statementTypeOther,
			"",
			"",
		},
		{
			"simple DROP",
			"DROP TABLE ks1.table1 blah",
			statementTypeOther,
			"",
			"",
		},
		{
			"empty",
			"",
			statementTypeOther,
			"",
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeUuidGenerator, err := GetDefaultTimeUuidGenerator()
			require.Nil(t, err)
			actual := inspectCqlQuery(tt.query, "", timeUuidGenerator)
			if actual.getStatementType() != tt.statementType {
				t.Errorf("inspectCqlQuery().isSelectStatement() actual = %v, expected %v", actual.getStatementType(), tt.statementType)
			}
			if actual.getKeyspaceName() != tt.keyspaceName {
				t.Errorf("inspectCqlQuery().getKeyspaceName() actual = %v, expected %v", actual.getKeyspaceName(), tt.keyspaceName)
			}
			if actual.getTableName() != tt.tableName {
				t.Errorf("inspectCqlQuery().getTableName() actual = %v, expected %v", actual.getTableName(), tt.tableName)
			}
		})
	}
}

func TestNowFunctionCalls(t *testing.T) {
	uid, _ := uuid.Parse("7872e70a-5a68-11eb-ae93-0242ac130002")
	tests := []struct {
		name                   string
		query                  string
		statementType          statementType
		replacement            uuid.UUID
		hasNow                 bool
		expectedWithLiteral    string
		expectedWithPositional string
		expectedWithNamed      string
		expectedReplacedTerms  []*term
	}{
		{
			"simple INSERT",
			"INSERT INTO ks1.table1 (foo) VALUES (now())",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo) VALUES (7872e70a-5a68-11eb-ae93-0242ac130002)",
			"INSERT INTO ks1.table1 (foo) VALUES (?)",
			"INSERT INTO ks1.table1 (foo) VALUES (:cloudgate__now)",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 37, 41), -1)},
		},
		{
			"simple INSERT with positional markers at start and end",
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (?, now(), now(), ?)",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (?, 7872e70a-5a68-11eb-ae93-0242ac130002, 7872e70a-5a68-11eb-ae93-0242ac130002, ?)",
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (?, ?, ?, ?)",
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (?, :cloudgate__now, :cloudgate__now, ?)", // invalid but doesn't matter here
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 59, 63), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 66, 70), 0)},
		},
		{
			"simple INSERT with positional markers at middle",
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (now(), ?, ?, now())",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (7872e70a-5a68-11eb-ae93-0242ac130002, ?, ?, 7872e70a-5a68-11eb-ae93-0242ac130002)",
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (?, ?, ?, ?)",
			"INSERT INTO ks1.table1 (foo1, foo2, foo3, foo4) VALUES (:cloudgate__now, ?, ?, :cloudgate__now)", // invalid but doesn't matter here
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 56, 60), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 69, 73), 1)},
		},
		{
			"qualified call INSERT",
			"INSERT INTO ks1.table1 (foo) VALUES (system.now())",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo) VALUES (7872e70a-5a68-11eb-ae93-0242ac130002)",
			"INSERT INTO ks1.table1 (foo) VALUES (?)",
			"INSERT INTO ks1.table1 (foo) VALUES (:cloudgate__now)",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("system", "now", 0, 37, 48), -1)},
		},
		{
			"qualified call with whitespace and quoted identifiers",
			"INSERT INTO ks1.table1 (foo) VALUES ( \"system\" . \"now\" ( ) )",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo) VALUES ( 7872e70a-5a68-11eb-ae93-0242ac130002 )",
			"INSERT INTO ks1.table1 (foo) VALUES ( ? )",
			"INSERT INTO ks1.table1 (foo) VALUES ( :cloudgate__now )",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("system", "now", 0, 38, 57), -1)},
		},
		{
			"cast INSERT",
			"INSERT INTO ks1.table1 (foo) VALUES ( ( uuid ) system.now())",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo) VALUES ( ( uuid ) 7872e70a-5a68-11eb-ae93-0242ac130002)",
			"INSERT INTO ks1.table1 (foo) VALUES ( ( uuid ) ?)",
			"INSERT INTO ks1.table1 (foo) VALUES ( ( uuid ) :cloudgate__now)",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("system", "now", 0, 47, 58), -1)},
		},
		{
			"other functions INSERT",
			"INSERT INTO ks1.table1 (foo, bar, qix) VALUES (now(), yesterday(), tomorrow())",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (foo, bar, qix) VALUES (7872e70a-5a68-11eb-ae93-0242ac130002, yesterday(), tomorrow())",
			"INSERT INTO ks1.table1 (foo, bar, qix) VALUES (?, yesterday(), tomorrow())",
			"INSERT INTO ks1.table1 (foo, bar, qix) VALUES (:cloudgate__now, yesterday(), tomorrow())",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 47, 51), -1)},
		},
		{
			"multiple occurrences INSERT",
			"INSERT INTO ks1.table1 (c1, c2, c3, c4) VALUES ( now(), now ( ), system.now(), \"system\" . \"now\" ( ))",
			statementTypeInsert,
			uid,
			true,
			"INSERT INTO ks1.table1 (c1, c2, c3, c4) VALUES ( 7872e70a-5a68-11eb-ae93-0242ac130002, 7872e70a-5a68-11eb-ae93-0242ac130002, 7872e70a-5a68-11eb-ae93-0242ac130002, 7872e70a-5a68-11eb-ae93-0242ac130002)",
			"INSERT INTO ks1.table1 (c1, c2, c3, c4) VALUES ( ?, ?, ?, ?)",
			"INSERT INTO ks1.table1 (c1, c2, c3, c4) VALUES ( :cloudgate__now, :cloudgate__now, :cloudgate__now, :cloudgate__now)",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 49, 53), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 56, 62), -1),
				NewFunctionCallTerm(NewFunctionCall("system", "now", 0, 65, 76), -1),
				NewFunctionCallTerm(NewFunctionCall("system", "now", 0, 79, 98), -1)},
		},
		{
			"INSERT inside BATCH",
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, now()) " +
				"DELETE FROM ks1.table1 WHERE c1 = 42 " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, now()) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, now()) " +
				"APPLY BATCH",
			statementTypeBatch,
			uid,
			true,
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, 7872e70a-5a68-11eb-ae93-0242ac130002) " +
				"DELETE FROM ks1.table1 WHERE c1 = 42 " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, 7872e70a-5a68-11eb-ae93-0242ac130002) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, 7872e70a-5a68-11eb-ae93-0242ac130002) " +
				"APPLY BATCH",
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, ?) " +
				"DELETE FROM ks1.table1 WHERE c1 = 42 " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, ?) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, ?) " +
				"APPLY BATCH",
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, :cloudgate__now) " +
				"DELETE FROM ks1.table1 WHERE c1 = 42 " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, :cloudgate__now) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, :cloudgate__now) " +
				"APPLY BATCH",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 56, 60), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 144, 148), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 195, 199), -1)},
		},
		{
			"BATCH with INSERTs, UPDATEs and DELETEs",
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, now()) " +
				"DELETE FROM ks1.table1 WHERE c1 = now() " +
				"UPDATE ks1.table1 SET c2 = now() WHERE c1 = now() " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, now()) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, now()) " +
				"APPLY BATCH",
			statementTypeBatch,
			uid,
			true,
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, 7872e70a-5a68-11eb-ae93-0242ac130002) " +
				"DELETE FROM ks1.table1 WHERE c1 = 7872e70a-5a68-11eb-ae93-0242ac130002 " +
				"UPDATE ks1.table1 SET c2 = 7872e70a-5a68-11eb-ae93-0242ac130002 WHERE c1 = 7872e70a-5a68-11eb-ae93-0242ac130002 " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, 7872e70a-5a68-11eb-ae93-0242ac130002) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, 7872e70a-5a68-11eb-ae93-0242ac130002) " +
				"APPLY BATCH",
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, ?) " +
				"DELETE FROM ks1.table1 WHERE c1 = ? " +
				"UPDATE ks1.table1 SET c2 = ? WHERE c1 = ? " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, ?) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, ?) " +
				"APPLY BATCH",
			"BEGIN BATCH " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, :cloudgate__now) " +
				"DELETE FROM ks1.table1 WHERE c1 = :cloudgate__now " +
				"UPDATE ks1.table1 SET c2 = :cloudgate__now WHERE c1 = :cloudgate__now " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, :cloudgate__now) " +
				"INSERT INTO ks1.table1 (c1, c2) VALUES (42, :cloudgate__now) " +
				"APPLY BATCH",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 56, 60), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 97, 101), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 130, 134), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 147, 151), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 197, 201), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 248, 252), -1)},
		},
		{
			"no occurrences",
			"INSERT INTO ks1.table1 (foo) VALUES ('bar')",
			statementTypeInsert,
			uid,
			false,
			"INSERT INTO ks1.table1 (foo) VALUES ('bar')",
			"INSERT INTO ks1.table1 (foo) VALUES ('bar')",
			"INSERT INTO ks1.table1 (foo) VALUES ('bar')",
			[]*term{},
		},
		{
			"update",
			"UPDATE ks1.table1 SET foo = 'bar' WHERE col = now()",
			statementTypeUpdate,
			uid,
			true,
			"UPDATE ks1.table1 SET foo = 'bar' WHERE col = 7872e70a-5a68-11eb-ae93-0242ac130002",
			"UPDATE ks1.table1 SET foo = 'bar' WHERE col = ?",
			"UPDATE ks1.table1 SET foo = 'bar' WHERE col = :cloudgate__now",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 46, 50), -1)},
		},
		{
			"delete",
			"DELETE FROM ks1.table1 WHERE col = now()",
			statementTypeDelete,
			uid,
			true,
			"DELETE FROM ks1.table1 WHERE col = 7872e70a-5a68-11eb-ae93-0242ac130002",
			"DELETE FROM ks1.table1 WHERE col = ?",
			"DELETE FROM ks1.table1 WHERE col = :cloudgate__now",
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 35, 39), -1)},
		},
		{
			"unknown statement",
			"CREATE TABLE foo",
			statementTypeOther,
			uid,
			false,
			"CREATE TABLE foo",
			"CREATE TABLE foo",
			"CREATE TABLE foo",
			[]*term{},
		},
		{
			"empty statement",
			"",
			statementTypeOther,
			uid,
			false,
			"",
			"",
			"",
			[]*term{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			info := inspectCqlQuery(tt.query, "", &fakeTimeUuidGenerator{uid: tt.replacement})
			assert.Equal(t, tt.statementType, info.getStatementType())
			assert.Equal(t, tt.hasNow, info.hasNowFunctionCalls())

			modifiedWithLiteral, replacedTerms1 := info.replaceNowFunctionCallsWithLiteral()
			modifiedWithPositional, replacedTerms2 := info.replaceNowFunctionCallsWithPositionalBindMarkers()
			modifiedWithNamed, replacedTerms3 := info.replaceNowFunctionCallsWithNamedBindMarkers()

			// check modified queries
			assert.Equal(t, tt.expectedWithLiteral, modifiedWithLiteral.getQuery())
			assert.Equal(t, tt.expectedWithPositional, modifiedWithPositional.getQuery())
			assert.Equal(t, tt.expectedWithNamed, modifiedWithNamed.getQuery())

			// modified queries should not have now() calls anymore
			assert.False(t, modifiedWithLiteral.hasNowFunctionCalls())
			assert.False(t, modifiedWithPositional.hasNowFunctionCalls())
			assert.False(t, modifiedWithNamed.hasNowFunctionCalls())

			// statement type should not change in modified queries
			assert.Equal(t, tt.statementType, modifiedWithLiteral.getStatementType())
			assert.Equal(t, tt.statementType, modifiedWithPositional.getStatementType())
			assert.Equal(t, tt.statementType, modifiedWithNamed.getStatementType())

			assert.Equal(t, tt.expectedReplacedTerms, replacedTerms1)
			assert.Equal(t, tt.expectedReplacedTerms, replacedTerms2)
			assert.Equal(t, tt.expectedReplacedTerms, replacedTerms3)
		})
	}
}

type fakeTimeUuidGenerator struct {
	uid uuid.UUID
}

func (recv *fakeTimeUuidGenerator) GetTimeUuid() uuid.UUID {
	return recv.uid
}
