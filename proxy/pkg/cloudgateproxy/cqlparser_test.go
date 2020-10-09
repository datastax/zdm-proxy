package cloudgateproxy

import (
	"encoding/binary"
	"errors"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"reflect"
	"testing"
	"time"
)

func TestInspectFrame(t *testing.T) {
	type args struct {
		f       *Frame
		psCache *PreparedStatementCache
		mh      metrics.IMetricsHandler
	}
	psCache := NewPreparedStatementCache()
	psCache.cache["WRITE"] = PreparedStatementInfo{true}
	psCache.cache["READ"] = PreparedStatementInfo{false}
	mh := mockMetricsHandler{}
	tests := []struct {
		name     string
		args     args
		expected interface{}
	}{
		// QUERY
		{"OpCodeQuery SELECT", args{mockFrame(OpCodeQuery, "SELECT blah"), psCache, mh}, forwardToOrigin},
		{"OpCodeQuery non SELECT", args{mockFrame(OpCodeQuery, "INSERT blah"), psCache, mh}, forwardToBoth},
		// PREPARE
		{"OpCodePrepare SELECT", args{mockFrame(OpCodePrepare, "SELECT blah"), psCache, mh}, forwardToOrigin},
		{"OpCodePrepare non SELECT", args{mockFrame(OpCodePrepare, "INSERT blah"), psCache, mh}, forwardToBoth},
		// EXECUTE
		{"OpCodeExecute read", args{mockExecuteFrame("READ"), psCache, mh}, forwardToOrigin},
		{"OpCodeExecute write", args{mockExecuteFrame("WRITE"), psCache, mh}, forwardToBoth},
		{"OpCodeExecute unknown", args{mockExecuteFrame("UNKNOWN"), psCache, mh}, forwardToBoth},
		// others
		{"OpCodeBatch", args{mockFrame(OpCodeBatch, "irrelevant"), psCache, mh}, forwardToBoth},
		{"OpCodeRegister", args{mockFrame(OpCodeRegister, "irrelevant"), psCache, mh}, forwardToBoth},
		{"OpCodeStartup", args{mockFrame(OpCodeStartup, "irrelevant"), psCache, mh}, forwardToBoth},
		{"OpCodeOptions", args{mockFrame(OpCodeOptions, "irrelevant"), psCache, mh}, forwardToBoth},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := inspectFrame(tt.args.f, tt.args.psCache, tt.args.mh)
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
		name     string
		query    string
		expected forwardDecision
	}{
		{"simple SELECT", "SELECT * FROM blah", forwardToOrigin},
		{"simple INSERT", "INSERT INTO blah", forwardToBoth},
		{"simple UPDATE", "UPDATE blah", forwardToBoth},
		{"simple DELETE", "DELETE FROM blah", forwardToBoth},
		{"simple BATCH", "BEGIN BATCH blah", forwardToBoth},
		{"simple CREATE", "CREATE blah", forwardToBoth},
		{"simple DROP", "DROP blah", forwardToBoth},
		{"whitespace", "   \t\r\n   SELECT", forwardToOrigin},
		{"single line comment dash", "-- blah  \n   SELECT", forwardToOrigin},
		{"single line comment dash Windows", "-- blah  \r\n   SELECT", forwardToOrigin},
		{"single line comment slash", "// blah  \n   SELECT", forwardToOrigin},
		{"single line comment slash Windows", "// blah  \r\n   SELECT", forwardToOrigin},
		{"multi line comment 1 line", "/* blah */  SELECT", forwardToOrigin},
		{"multi line comment 2 lines", "/* blah  \t\r\n */  SELECT", forwardToOrigin},
		{"many comments", "-- comment1 \n // comment 2 \n /* comment 2\t\r\n */  SELECT", forwardToOrigin},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := inspectCqlQuery(tt.query); actual != tt.expected {
				t.Errorf("inspectCqlQuery() actual = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

func TestReadLongString(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple string", []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, "hello"},
		{"empty string", []byte{0, 0, 0, 0}, ""},
		{"cannot read length", []byte{0, 0, 0}, errors.New("not enough bytes to read a long string")},
		{"cannot read string", []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c}, errors.New("not enough bytes to read a long string")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readLongString(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readLongString() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("readLongString() actual = %v, want %v", actual, tt.expected)
			}
		})
	}
}

func TestReadShortBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected interface{}
	}{
		{"simple array", []byte{0, 5, 1, 2, 3, 4, 5}, []byte{1, 2, 3, 4, 5}},
		{"empty array", []byte{0, 0}, []byte{}},
		{"cannot read length", []byte{0}, errors.New("not enough bytes to read a short bytes")},
		{"cannot read array", []byte{0, 5, 1, 2, 3, 4}, errors.New("not enough bytes to read a short bytes")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := readShortBytes(tt.data)
			if err != nil {
				if !reflect.DeepEqual(err, tt.expected) {
					t.Errorf("readShortBytes() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("readShortBytes() actual = %v, expected %v", actual, tt.expected)
			}
		})
	}
}
