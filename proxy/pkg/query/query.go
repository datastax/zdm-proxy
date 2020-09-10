package query

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/riptano/cloud-gate/proxy/pkg/cqlparser"
	"github.com/riptano/cloud-gate/proxy/pkg/frame"

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
	EXECUTE	 = Type("execute")
	BATCH    = Type("batch")
	REGISTER = Type("register")
	OPTIONS  = Type("options")
	MISC     = Type("misc")

	ORIGIN_CASSANDRA = DestinationCluster("originCassandra")
	ASTRA            = DestinationCluster("astra")

	// Bit within the flags byte of a query denoting whether the query includes a timestamp
	timestampBit = 0x20
)

// Type represents the type of query.
type Type string

// This indicates the cluster this query will be executed on
type DestinationCluster string

// Query represents a query sent to the proxy, destined to be executed on the Astra database.
type Query struct {
	Timestamp uint64
	Stream    uint16
	//Table     *migration.Table		// it looks like this was only necessary to coordinate the handling of the table as part of the migration service

	Type            Type
	Query           []byte
	Keyspace		string
	Opcode          byte
	SourceIPAddress string
	Destination 	DestinationCluster
	WG              *sync.WaitGroup	// TODO check what this is used for - might be related to the coordination of the migration service?

	Paths []string
}

// New returns a new query struct with the Timestamp set to the struct's creation time.
//func New(table *migration.Table, queryType Type, f *frame.Frame, source string, parsedPaths []string) *Query {
func New(queryType Type, f *frame.Frame, source string, destination DestinationCluster, parsedPaths []string, keyspace string) *Query {
	var opcode byte
	if len(f.RawBytes) >= 5 {
		opcode = f.RawBytes[4]
	}

	return &Query{
		Timestamp: uint64(time.Now().UnixNano() / 1000000),
		//Table:     table,
		Type:            queryType,
		Query:           f.RawBytes,
		Keyspace: 		 keyspace,
		Stream:          f.Stream,
		Opcode:          opcode,
		SourceIPAddress: source,
		Destination: 	 destination,
		Paths:           parsedPaths,
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

	// Statement already includes timestamp
	if flags&timestampBit == timestampBit {
		return q
	}

	// Set the timestamp bit to 1
	q.Query[flagsByte] = flags | 0x20

	// Add timestamp to end of query
	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, q.Timestamp)
	q.Query = append(q.Query, timestamp...)

	// Update length of body if not BATCH statement
	if q.Opcode != 0x0d {
		bodyLen := binary.BigEndian.Uint32(q.Query[5:9]) + 8
		binary.BigEndian.PutUint32(q.Query[5:9], bodyLen)
	}

	return q
}

func GetDestinationCluster(toAstra bool) DestinationCluster {
	if toAstra {
		return ASTRA
	} else {
		return ORIGIN_CASSANDRA
	}
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
