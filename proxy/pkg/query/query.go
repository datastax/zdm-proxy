package query

import (
	"encoding/binary"
	"strings"
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
	return &Query{
		Timestamp: uint64(time.Now().UnixNano() / 1000000),
		Table:     table,
		Type:      queryType,
		Query:     f.RawBytes,
		Stream:    f.Stream,
		Opcode:    f.RawBytes[4],
		Source:    source,
		Paths:     parsedPaths,
	}
}

// usingTimestamp adds a timestamp to a query, if one is not already present.
// Supports BATCH, UPDATE, and INSERT queries.
func (q *Query) UsingTimestamp() *Query {
	flagsByte := q.getFlagsByte()
	if flagsByte == -1 {
		log.Errorf("opcode %d not supported for UsingTimestamp() on query %d", q.Opcode, q.Stream)
		return q
	}

	flags := q.Query[flagsByte]

	// Query already includes timestamp, ignore
	// Byte 0x20 of the flags portion of the query represent whether or not a timestamp
	// will be included with this query
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
		queryLen := binary.BigEndian.Uint32(q.Query[9:11])
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

// addKeyspace adds an explicit keyspace declaration to a query.
// For example, if the user is in keyspace 'codebase' and they run:
// 		INSERT INTO tasks(id, task) VALUES(now(), 'task')
// this function will change the query to
// 		INSERT INTO codebase.tasks(id, task) VALUES(now(), 'task')
// Assumes Query is a BATCH or QUERY
func (q *Query) AddKeyspace(keyspace string) *Query {
	if q.Opcode == 0x0d {
		// BATCH query
		return q.batchQueryAddKeyspace(keyspace)
	} else {
		// All others
		return q.nonBatchAddKeyspace(keyspace)
	}
}

func (q *Query) nonBatchAddKeyspace(keyspace string) *Query {
	// if QUERY
	tablename := strings.Split(q.Paths[0], "/")[3]
	if strings.Contains(tablename, ".") {
		return q
	}

	// Find table in original query
	index := strings.Index(string(q.Query), tablename)

	before := make([]byte, index)
	copy(before, q.Query[:index])
	after := q.Query[index:]

	// Rebuild query
	var tablePrefix []byte
	if isLower(keyspace) {
		tablePrefix = []byte(keyspace + ".")
	} else {
		tablePrefix = []byte("\"" + keyspace + "\".")
	}

	updatedQuery := append(before, tablePrefix...)
	updatedQuery = append(updatedQuery, after...)

	// Update body length
	bodyLen := binary.BigEndian.Uint32(updatedQuery[5:9]) + uint32(len(tablePrefix))
	binary.BigEndian.PutUint32(updatedQuery[5:9], bodyLen)

	//update query string length
	queryLen := binary.BigEndian.Uint32(updatedQuery[9:13]) + uint32(len(tablePrefix))
	binary.BigEndian.PutUint32(updatedQuery[9:13], queryLen)

	q.Query = updatedQuery
	return q
}

func (q *Query) batchQueryAddKeyspace(keyspace string) *Query {
	numQueries := binary.BigEndian.Uint16(q.Query[10:12])
	offset := 12
	for i := 0; i < int(numQueries); i++ {
		kind := q.Query[offset]
		if kind == 0 {
			// full query string
			queryLenIndex := offset + 1
			queryLen := int(binary.BigEndian.Uint32(q.Query[offset+1 : offset+5]))
			query := string(q.Query[offset+5 : offset+5+queryLen])

			tablename := strings.Split(q.Paths[i], "/")[3]
			var bodyLen uint32
			if strings.Contains(tablename, ".") {
				bodyLen = uint32(queryLen)
			} else {
				tablenameIndex := strings.Index(query, tablename) + offset + 5

				before := make([]byte, tablenameIndex)
				copy(before, q.Query[:tablenameIndex])
				after := q.Query[tablenameIndex:]

				// Rebuild query
				var tablePrefix []byte
				if isLower(keyspace) {
					tablePrefix = []byte(keyspace + ".")
				} else {
					tablePrefix = []byte("\"" + keyspace + "\".")
				}
				updatedQuery := append(before, tablePrefix...)
				updatedQuery = append(updatedQuery, after...)

				// Update query length
				bodyLen := uint32(queryLen) + uint32(len(tablePrefix))
				binary.BigEndian.PutUint32(updatedQuery[queryLenIndex:queryLenIndex+4], bodyLen)

				q.Query = updatedQuery
			}

			offset = offset + 5 + int(bodyLen)
			offset = cqlparser.ReadPastBatchValues(q.Query, offset)
		} else if kind == 1 {
			// prepared query id
			idLen := int(binary.BigEndian.Uint16(q.Query[offset+1 : offset+3]))
			offset = offset + 3 + idLen
			offset = cqlparser.ReadPastBatchValues(q.Query, offset)
		}
	}
	return q
}

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
