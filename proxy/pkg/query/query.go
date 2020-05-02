package query

import (
	"cloud-gate/proxy/pkg/cqlparser"
	"encoding/binary"
	"strings"
	"sync"
	"time"
	"unicode"

	"cloud-gate/migration/migration"
	"cloud-gate/proxy/pkg/frame"
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
)

type Type string

type Query struct {
	Timestamp uint64
	Stream    uint16
	Table     *migration.Table

	Type   Type
	Query  []byte
	Source string
	WG     *sync.WaitGroup

	Paths []string
}

func New(table *migration.Table, queryType Type, f *frame.Frame, source string, parsedPaths []string) *Query {
	return &Query{
		Timestamp: uint64(time.Now().UnixNano() / 1000000),
		Table:     table,
		Type:      queryType,
		Query:     f.RawBytes,
		Stream:    f.Stream,
		Source:    source,
		Paths:     parsedPaths,
	}
}

// usingTimestamp will add a timestamp within the query, if one is not already present.
// Handles BATCH, UPDATE, and INSERT statements
func (q *Query) UsingTimestamp() *Query {
	opcode := q.Query[4]

	// if BATCH, jump to the end of the query to find the FLAG byte
	var flagBit int
	if opcode == 0x0d {
		numQueries := binary.BigEndian.Uint16(q.Query[10:12])
		offset := 12
		for i := 0; i < int(numQueries); i++ {
			kind := q.Query[offset]
			if kind == 0 {
				// full query string
				queryLen := int(binary.BigEndian.Uint32(q.Query[offset+1 : offset+5]))
				offset = offset + 5 + queryLen
				offset = cqlparser.ReadPastBatchValues(q.Query, offset)
			} else if kind == 1 {
				// prepared query id
				idLen := int(binary.BigEndian.Uint16(q.Query[offset+1 : offset+3]))
				offset = offset + 3 + idLen
				offset = cqlparser.ReadPastBatchValues(q.Query, offset)
			}
		}
		flagBit = offset + 2
	} else {
		//index represents start of <query_parameters> in binary protocol
		var index int
		if opcode == 0x07 {
			//if QUERY
			queryLen := binary.BigEndian.Uint32(q.Query[9:13])
			index = 13 + int(queryLen)
		} else if opcode == 0x0a {
			//if EXECUTE
			queryLen := binary.BigEndian.Uint32(q.Query[9:11])
			index = 11 + int(queryLen)
		}
		flagBit = index + 2
	}

	// Query already includes timestamp, ignore
	// Byte 0x20 of the flags portion of the query represent whether or not a timestamp
	// will be included with this query
	if q.Query[flagBit]&0x20 == 0x20 {
		// TODO: Ensure we can keep the original timestamp & we don't need to alter anything
		// binary.BigEndian.PutUint64(q.Query[len(q.Query) - 8:], q.Timestamp)
		return q
	}

	// Set the timestamp bit (0x20) of flags to 1
	q.Query[flagBit] = q.Query[flagBit] | 0x20

	// Add timestamp to end of query
	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, q.Timestamp)
	q.Query = append(q.Query, timestamp...)

	// Update length of body if not BATCH statement
	if opcode != 0x0d {
		bodyLen := binary.BigEndian.Uint32(q.Query[5:9]) + 8
		binary.BigEndian.PutUint32(q.Query[5:9], bodyLen)
	}

	return q
}

// TODO: Make cleaner / more efficient
// addKeyspace will explicity add the keyspace to a query, if not present.
// Assumes Query is a BATCH or QUERY
// For example, if the user is in keyspace 'codebase' and they run:
// 		INSERT INTO tasks(id, task) VALUES(now(), 'task')
// this function will change the query to
// 		INSERT INTO codebase.tasks(id, task) VALUES(now(), 'task')
func (q *Query) AddKeyspace(keyspace string) *Query {
	opcode := q.Query[4]

	// if BATCH, insert keyspace into every <query> that needs it
	if opcode == 0x0d {
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
	} else {
		// if QUERY
		tablename := strings.Split(q.Paths[0], "/")[3]
		if strings.Contains(tablename, ".") {
			return q
		} else {
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
	}
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