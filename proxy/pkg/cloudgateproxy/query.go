package cloudgateproxy

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

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

	// Bit within the flags byte of a query denoting whether the query includes a timestamp
	timestampBit = 0x20
)

// Type represents the type of query.
type Type string

// Query represents a query sent to the proxy, destined to be executed on the TargetCassandra database.
type Query struct {
	Timestamp	uint64
	Stream		uint16
	Type        Type
	Query       []byte
	Opcode      byte
}

// New returns a new query struct with the Timestamp set to the struct's creation time.
func NewQuery(	queryType Type,
				f *Frame) *Query {
	var opcode byte
	if len(f.RawBytes) >= 5 {
		opcode = f.RawBytes[4]
	}

	return &Query{
		Timestamp: uint64(time.Now().UnixNano() / 1000000),
		Stream:          f.Stream,
		Type:            queryType,
		Query:           f.RawBytes,
		Opcode:          opcode,
	}
}

func createQuery(f *Frame, paths []string, psCache *PreparedStatementCache, isWriteRequest bool) (*Query, error){
	var q *Query

	if paths[0] == UnknownPreparedQueryPath {
		// TODO is this handling correct?
		return q, fmt.Errorf("encountered unknown prepared query for stream %d, ignoring", f.Stream)
	}

	if len(paths) > 1 {
		log.Tracef("batch query")
		return createBatchQuery(f), nil
	}

	// currently handles query and prepare statements that involve 'use, insert, update, delete, and truncate'
	fields := strings.Split(paths[0], "/")
	log.Tracef("Path %s split into following fields: %s", paths[0], fields)
	// NOTE: fields[0] is an empty element. the first meaningful element in the path is fields[1]
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
		log.Tracef("fields[%d]: %s", i, fields[i])
	}
	log.Tracef("fields: %s", fields)

	if len(fields) > 2 {
		log.Tracef("at least two fields found")
		switch fields[1] {
		case "prepare":
			log.Tracef("prepare statement query")
			return createPrepareQuery(f, psCache, isWriteRequest), nil
		case "query":
			log.Tracef("statement query")
			queryType := Type(fields[2])
			log.Tracef("fields: %s", queryType)
			switch queryType {
			case USE:
				log.Tracef("query type use")
				return createUseQuery(f), nil
			case INSERT, UPDATE, DELETE, TRUNCATE:
				log.Tracef("query type insert update delete or truncate")
				return createWriteQuery(queryType, f), nil
			case SELECT:
				log.Tracef("query type select")
				return createReadQuery(f), nil
			}
		case "batch":
			return createBatchQuery(f), nil
		}
	} else {
		// path is '/opcode' case
		// FIXME: decide if there are any cases we need to handle here

		log.Tracef("len(fields) < 2: %s, %v", fields, len(fields))
		switch fields[1] {
		case "execute":
			log.Tracef("Execute")
			// create and return execute query. this will just be of type EXECUTE, containing the raw request bytes. nothing else is needed.
			q = NewQuery(EXECUTE, f)
		case "register":
			log.Tracef("Register")
			q = NewQuery(REGISTER, f)
		case "options":
			log.Tracef("Options")
			q = NewQuery(OPTIONS, f)
		default:
			log.Tracef("Misc query")
			q = NewQuery(MISC, f)
		}

	}
	return q, nil
}

func createPrepareQuery(f *Frame, psCache *PreparedStatementCache, isWriteRequest bool) *Query {
	q := NewQuery(PREPARE, f)
	psCache.trackStatementToBePrepared(q, isWriteRequest)
	return q
}

func createUseQuery(f *Frame) *Query {
	return NewQuery(USE, f)
}

func createWriteQuery(queryType Type, f *Frame) *Query {
return NewQuery(queryType, f).UsingTimestamp()
}

func createReadQuery(f *Frame) *Query {
	return NewQuery(SELECT, f).UsingTimestamp()
}

func createBatchQuery(f *Frame) *Query {
	return NewQuery(BATCH, f).UsingTimestamp()
}

// TODO should the proxy be doing this?? either it is client-driven or it is cluster-driven...
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
			offset = ReadPastBatchValues(frame, offset)
		} else if kind == 1 {
			// prepared query id
			idLen := int(binary.BigEndian.Uint16(frame[offset+1 : offset+3]))
			offset = offset + 3 + idLen
			offset = ReadPastBatchValues(frame, offset)
		}
	}
	return offset + 2
}
