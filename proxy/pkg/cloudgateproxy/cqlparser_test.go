package cloudgateproxy

import (
	"encoding/binary"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestInspectFrame(t *testing.T) {
	type args struct {
		f       *Frame
		psCache *PreparedStatementCache
		mh      metrics.IMetricsHandler
		km      *atomic.Value
	}
	psCache := NewPreparedStatementCache()
	psCache.cache["BOTH"] = preparedStatementInfo{forwardToBoth}
	psCache.cache["ORIGIN"] = preparedStatementInfo{forwardToOrigin}
	psCache.cache["TARGET"] = preparedStatementInfo{forwardToTarget}
	mh := mockMetricsHandler{}
	km := new(atomic.Value)
	tests := []struct {
		name     string
		args     args
		expected interface{}
	}{
		// QUERY
		{"OpCodeQuery SELECT", args{mockFrame(OpCodeQuery, "SELECT blah FROM ks1.t1"), psCache, mh, km}, forwardToOrigin},
		{"OpCodeQuery SELECT system.local", args{mockFrame(OpCodeQuery, "SELECT * FROM system.local"), psCache, mh, km}, forwardToTarget},
		{"OpCodeQuery SELECT system.peers", args{mockFrame(OpCodeQuery, "SELECT * FROM system.peers"), psCache, mh, km}, forwardToTarget},
		{"OpCodeQuery SELECT system.peers_v2", args{mockFrame(OpCodeQuery, "SELECT * FROM system.peers_v2"), psCache, mh, km}, forwardToTarget},
		{"OpCodeQuery non SELECT", args{mockFrame(OpCodeQuery, "INSERT blah"), psCache, mh, km}, forwardToBoth},
		// PREPARE
		{"OpCodePrepare SELECT", args{mockFrame(OpCodePrepare, "SELECT blah FROM ks1.t1"), psCache, mh, km}, forwardToOrigin},
		{"OpCodePrepare SELECT system.local", args{mockFrame(OpCodePrepare, "SELECT * FROM system.local"), psCache, mh, km}, forwardToTarget},
		{"OpCodePrepare SELECT system.peers", args{mockFrame(OpCodePrepare, "SELECT * FROM system.peers"), psCache, mh, km}, forwardToTarget},
		{"OpCodePrepare SELECT system.peers_v2", args{mockFrame(OpCodePrepare, "SELECT * FROM system.peers_v2"), psCache, mh, km}, forwardToTarget},
		{"OpCodePrepare non SELECT", args{mockFrame(OpCodePrepare, "INSERT blah"), psCache, mh, km}, forwardToBoth},
		// EXECUTE
		{"OpCodeExecute origin", args{mockExecuteFrame("ORIGIN"), psCache, mh, km}, forwardToOrigin},
		{"OpCodeExecute target", args{mockExecuteFrame("TARGET"), psCache, mh, km}, forwardToTarget},
		{"OpCodeExecute both", args{mockExecuteFrame("BOTH"), psCache, mh, km}, forwardToBoth},
		{"OpCodeExecute unknown", args{mockExecuteFrame("UNKNOWN"), psCache, mh, km}, forwardToBoth},
		// others
		{"OpCodeBatch", args{mockFrame(OpCodeBatch, "irrelevant"), psCache, mh, km}, forwardToBoth},
		{"OpCodeRegister", args{mockFrame(OpCodeRegister, "irrelevant"), psCache, mh, km}, forwardToBoth},
		{"OpCodeStartup", args{mockFrame(OpCodeStartup, "irrelevant"), psCache, mh, km}, forwardToBoth},
		{"OpCodeOptions", args{mockFrame(OpCodeOptions, "irrelevant"), psCache, mh, km}, forwardToBoth},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := inspectFrame(tt.args.f, tt.args.psCache, tt.args.mh, tt.args.km)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("inspectFrame() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("inspectFrame() actual = %v, want %v", actual, tt.expected)
			}
		})
	}
}

type mockMetricsHandler struct{}

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) IncrementCountByOne(mn metrics.MetricsName) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) DecrementCountByOne(mn metrics.MetricsName) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) AddToCount(mn metrics.MetricsName, valueToAdd int) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) SubtractFromCount(mn metrics.MetricsName, valueToSubtract int) error {
	return nil
}

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) TrackInHistogram(mn metrics.MetricsName, timeToTrack time.Time) error {
	return nil
}
func (h mockMetricsHandler) UnregisterAllMetrics() error { return nil }

func mockFrame(opcode OpCode, query string) *Frame {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, uint32(len(query)))
	body = append(body, []byte(query)...)
	return &Frame{
		Direction: 0,
		Version:   0,
		Flags:     0,
		StreamId:  1,
		Opcode:    opcode,
		Length:    5,
		Body:      body,
		RawBytes:  nil, // not needed
	}
}

func mockExecuteFrame(preparedId string) *Frame {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, uint16(len(preparedId)))
	body = append(body, preparedId...)
	return &Frame{
		Direction: 0,
		Version:   0,
		Flags:     0,
		StreamId:  1,
		Opcode:    OpCodeExecute,
		Length:    5,
		Body:      body,
		RawBytes:  nil, // not needed
	}
}

func TestInspectCqlQuery(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		statementType statementType
		keyspaceName  string
		tableName     string
	}{
		// SELECT statements
		{"simple SELECT", "SELECT foo, bar, qix FROM table1 WHERE blah;", statementTypeSelect, "", "table1"},
		{"qualified SELECT", "SELECT foo, bar, qix FROM ks1.table1 WHERE blah;", statementTypeSelect, "ks1", "table1"},
		{"simple SELECT star", "SELECT * FROM table1", statementTypeSelect, "", "table1"},
		{"qualified SELECT star", "SELECT * FROM ks1.TABLE1", statementTypeSelect, "ks1", "table1"},
		{"quoted SELECT", "SELECT foo, bar, qix FROM \"MyTable\"", statementTypeSelect, "", "MyTable"},
		{"quoted qualified SELECT", "SELECT foo, bar, qix FROM \"MyKeyspace\" . \"MyTable\"", statementTypeSelect, "MyKeyspace", "MyTable"},
		{"quoted qualified SELECT with quotes", "SELECT foo, bar, qix FROM \"MyKeyspace\" . \"My\"\"Table\"", statementTypeSelect, "MyKeyspace", "My\"Table"},
		{"complex SELECTors", "SELECT foo, \"BAR\" AS bar, 'literal', $$ plsql-style literal $$, -NaN, 0.1, true, PT2S, 0xcafebabe, 97bda55b-6175-4c39-9e04-7c0205c709dc, system.now(), CAST(qix AS varchar), (list<varchar>) [ 'a', 'b' ] FROM ks1 . table1", statementTypeSelect, "ks1", "table1"},
		{"json SELECT", "SELECT JSON DISTINCT foo, bar, qix FROM table1 WHERE blah;", statementTypeSelect, "", "table1"},
		// whitespace and comments before SELECT statement
		{"whitespace before SELECT", "   \t\r\n   SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"single line comment dash", "-- blah  \n   SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"single line comment dash Windows", "-- blah  \r\n   SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"single line comment slash", "// blah  \n   SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"single line comment slash Windows", "// blah  \r\n   SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"multi line comment 1 line", "/* blah */  SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"multi line comment 2 lines", "/* blah  \t\r\n */  SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		{"many comments", "-- comment1 \n // comment 2 \n /* comment 2\t\r\n */  SELECT foo, bar FROM table1", statementTypeSelect, "", "table1"},
		// Other statements: keyspace and table names not detected
		{"simple USE", "USE ks1", statementTypeOther, "", ""},
		{"simple INSERT", "INSERT INTO blah", statementTypeOther, "", ""},
		{"simple UPDATE", "UPDATE blah", statementTypeOther, "", ""},
		{"simple DELETE", "DELETE FROM blah", statementTypeOther, "", ""},
		{"simple BATCH", "BEGIN BATCH blah", statementTypeOther, "", ""},
		{"simple CREATE", "CREATE blah", statementTypeOther, "", ""},
		{"simple DROP", "DROP blah", statementTypeOther, "", ""},
		{"empty", "", statementTypeOther, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := inspectCqlQuery(tt.query)
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
