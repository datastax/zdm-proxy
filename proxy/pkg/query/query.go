package query

import (
	"encoding/binary"
	"sync"
	"time"
	"unicode"

	"cloud-gate/migration/migration"
	"cloud-gate/proxy/pkg/cqlparser"
	"cloud-gate/proxy/pkg/frame"

	log "github.com/sirupsen/logrus"
)

const (
	SELECT   = Type("select")
	USE      = Type("use")
	INSERT   = Type("insert")
	UPDATE   = Type("update")
	DELETE   = Type("delete")
	TRUNCATE = Type("truncate")
	PREPARE  = Type("prepare")
	BATCH    = Type("batch")
	MISC     = Type("misc")

	// Bit within the flags byte of a query denoting whether the query includes a timestamp
	timestampBit = 0x20
)

// Type represents the type of query.
type Type string

// Query represents a query sent to the proxy, destined to be executed on the Astra database.
type Query struct {
	Timestamp uint64
	Stream    uint16
	Table     *migration.Table

	Type   Type
	Query  []byte
	Opcode byte
	Source string
	WG     *sync.WaitGroup

	Paths []string
}

// New returns a new query struct with the Timestamp set to the struct's creation time.
func New(table *migration.Table, queryType Type, f *frame.Frame, source string, parsedPaths []string) *Query {
	var opcode byte
	if len(f.RawBytes) >= 5 {
		opcode = f.RawBytes[4]
	}

	return &Query{
		Timestamp: uint64(time.Now().UnixNano() / 1000000),
		Table:     table,
		Type:      queryType,
		Query:     f.RawBytes,
		Stream:    f.Stream,
		Opcode:    opcode,
		Source:    source,
		Paths:     parsedPaths,
	}
}

// UsingTimestamp adds a timestamp to a query, if one is not already present.
// Supports BATCH, UPDATE, and INSERT queries.
func (q *Query) UsingTimestamp() *Query {
	flagsByte := q.getFlagsByte()
	if flagsByte == -1 {
		log.Errorf("opcode %d not supported for UsingTimestamp() on query %d", q.Opcode, q.Stream)
		return q
	}

	flags := q.Query[flagsByte]

	// Query already includes timestamp
	if flags&timestampBit == timestampBit {
		return q
	}

	// Set the timestamp bit to 1
	q.Query[flagsByte] = flags | 0x20

	// Add timestamp to end of query
	timestamp := make([]byte, 4)
	binary.BigEndian.PutUint64(timestamp, q.Timestamp)
	q.Query = append(q.Query, timestamp...)

	// Update length of body if not BATCH statement
	if q.Opcode != 0x0d {
		bodyLen := binary.BigEndian.Uint32(q.Query[5:9]) + 4
		binary.BigEndian.PutUint32(q.Query[5:9], bodyLen)
	}

	return q
}

func (q *Query) getFlagsByte() int {
	switch q.Opcode {
	case 0x07:
		// Normal query
		queryLen := binary.BigEndian.Uint32(q.Query[9:13])
		return 15 + int(queryLen)
	case 0x0a:
		// EXECUTE query
		queryLen := binary.BigEndian.Uint16(q.Query[9:11])
		return 13 + int(queryLen)
	case 0x0d:
		// BATCH query
		return flagsByteFromBatch(q.Query)
	}

	return -1
}

func flagsByteFromBatch(frame []byte) int {
	numQueries := binary.BigEndian.Uint16(frame[10:12])
	offset := 12
	for i := 0; i < int(numQueries); i++ {
		kind := frame[offset]
		if kind == 0 {
			// full query string
			queryLen := int(binary.BigEndian.Uint32(frame[offset+1 : offset+5]))
			offset = offset + 5 + queryLen
			offset = cqlparser.ReadPastBatchValues(frame, offset)
		} else if kind == 1 {
			// prepared query id
			idLen := int(binary.BigEndian.Uint16(frame[offset+1 : offset+3]))
			offset = offset + 3 + idLen
			offset = cqlparser.ReadPastBatchValues(frame, offset)
		}
	}
	return offset + 2
}

// WithWaitGroup assigns a WaitGroup to to a Query
func (q *Query) WithWaitGroup(waitgroup *sync.WaitGroup) *Query {
	q.WG = waitgroup
	return q
}

func isLower(s string) bool {
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}
