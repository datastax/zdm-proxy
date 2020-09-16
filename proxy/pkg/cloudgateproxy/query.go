package cloudgateproxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
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

	ORIGIN_CASSANDRA = DestinationCluster("originCassandra")
	TARGET_CASSANDRA = DestinationCluster("targetCassandra")

	// Bit within the flags byte of a query denoting whether the query includes a timestamp
	timestampBit = 0x20
)

// Type represents the type of query.
type Type string

// This indicates the cluster this query will be executed on
type DestinationCluster string

// Query represents a query sent to the proxy, destined to be executed on the TargetCassandra database.
type Query struct {
	Timestamp uint64
	Stream    uint16
	//Table     *migration.Table		// it looks like this was only necessary to coordinate the handling of the table as part of the migration service

	Type            Type
	Query           []byte
	Keyspace        string
	Opcode          byte
	SourceIPAddress string
	Destination     DestinationCluster
	WG              *sync.WaitGroup	// TODO check what this is used for - might be related to the coordination of the migration service?

	Paths []string
}

// New returns a new query struct with the Timestamp set to the struct's creation time.
//func New(table *migration.Table, queryType Type, f *frame.Frame, source string, parsedPaths []string) *Query {
func NewQuery(queryType Type, f *Frame, source string, destination DestinationCluster, parsedPaths []string, keyspace string) *Query {
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

func GetDestinationCluster(toTargetCassandra bool) DestinationCluster {
	if toTargetCassandra {
		return TARGET_CASSANDRA
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

func (p *CloudgateProxy) createQuery(f *Frame, clientIPAddress string, paths []string, toTargetCassandra bool) (*Query, error){
	var q *Query

	if paths[0] == UnknownPreparedQueryPath {
		// TODO is this handling correct?
		return q, fmt.Errorf("encountered unknown prepared query for stream %d, ignoring", f.Stream)
	}

	if len(paths) > 1 {
		log.Debugf("batch query")
		return p.createBatchQuery(f, clientIPAddress, paths, toTargetCassandra)
	}

	// currently handles query and prepare statements that involve 'use, insert, update, delete, and truncate'
	fields := strings.Split(paths[0], "/")
	log.Debugf("$$$$$$$$ path %s split into following fields: %s", paths[0], fields)
	// NOTE: fields[0] is an empty element. the first meaningful element in the path is fields[1]
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
		log.Debugf("fields[%d]: %s", i, fields[i])
	}
	log.Debugf("fields: %s", fields)

	if len(fields) > 2 {
		log.Debugf("at least two fields found")
		switch fields[1] {
		case "prepare":
			log.Debugf("prepare statement query")
			return p.createPrepareQuery(fields[3], f, clientIPAddress, paths, toTargetCassandra)
		case "query":
			log.Debugf("statement query")
			queryType := Type(fields[2])
			log.Debugf("fields: %s", queryType)
			switch queryType {
			case USE:
				log.Debugf("query type use")
				return p.createUseQuery(fields[3], f, clientIPAddress, paths, toTargetCassandra)
			case INSERT, UPDATE, DELETE, TRUNCATE:
				log.Debugf("query type insert update delete or truncate")
				return p.createWriteQuery(fields[3], queryType, f, clientIPAddress, paths, toTargetCassandra)
			case SELECT:
				log.Debugf("query type select")
				p.Metrics.IncrementReads()
				return p.createReadQuery(fields[3], f, clientIPAddress, paths)
			}
		case "batch":
			return p.createBatchQuery(f, clientIPAddress, paths, toTargetCassandra)
		}
	} else {
		// path is '/opcode' case
		// FIXME: decide if there are any cases we need to handle here
		var destination DestinationCluster
		if toTargetCassandra {
			destination = ORIGIN_CASSANDRA
		} else {
			destination = TARGET_CASSANDRA
		}

		log.Debugf("len(fields) < 2: %s, %v", fields, len(fields))
		switch fields[1] {
		case "execute":
			log.Debugf("Execute")
			// create and return execute query. this will just be of type EXECUTE, containing the raw request bytes. nothing else is needed.
			q = NewQuery(EXECUTE, f, clientIPAddress, destination, paths, "")
		case "register":
			log.Debugf("Register!!")
			q = NewQuery(REGISTER, f, clientIPAddress, destination, paths, "")
		case "options":
			log.Debugf("Options!!")
			q = NewQuery(OPTIONS, f, clientIPAddress, destination, paths, "")
		default:
			log.Debugf("Misc query")
			q = NewQuery(MISC, f, clientIPAddress, destination, paths, "")
		}

	}
	return q, nil
}

// TODO is the fromClause really necessary here? can it be replaced or extracted otherwise?
func (p *CloudgateProxy) createPrepareQuery(fromClause string, f *Frame, clientIPAddress string, paths []string, toTargetCassandra bool) (*Query, error) {
	var q *Query
	keyspace := extractKeyspace(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if toTargetCassandra {
			keyspace = p.currentTargetCassandraKeyspacePerClient[clientIPAddress]
		} else {
			keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = NewQuery(PREPARE, f, clientIPAddress, GetDestinationCluster(toTargetCassandra), paths, keyspace)
	return q, nil
}

// TODO is the keyspace parameter really necessary here? can it be replaced or extracted otherwise?
func (p *CloudgateProxy) createUseQuery(keyspace string, f *Frame, clientIPAddress string,
	paths []string, toTargetCassandra bool) (*Query, error) {
	var q *Query

	if keyspace == "" {
		log.Errorf("Missing or invalid keyspace!")
		return q, errors.New("missing or invalid keyspace")
	}

	// TODO perhaps replace this logic with call to function that parses keyspace name (sthg like extractKeyspace)
	// Cassandra assumes case-insensitive unless keyspace is encased in quotation marks
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = keyspace[1 : len(keyspace)-1]
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	p.lock.Lock()
	if toTargetCassandra {
		p.currentTargetCassandraKeyspacePerClient[clientIPAddress] = keyspace
	} else {
		p.currentOriginCassandraKeyspacePerClient[clientIPAddress] = keyspace
	}
	p.lock.Unlock()

	q = NewQuery(USE, f, clientIPAddress, GetDestinationCluster(toTargetCassandra), paths, keyspace)

	return q, nil
}

// handleWriteQuery can handle QUERY and EXECUTE opcodes of type INSERT, UPDATE, DELETE, TRUNCATE
// TODO tidy up signature. it should be possible to pass fewer parameters with some refactoring
func (p *CloudgateProxy) createWriteQuery(fromClause string, queryType Type, f *Frame,
	clientIPAddress string, paths []string, toTargetCassandra bool) (*Query, error) {
	var q *Query

	keyspace := extractKeyspace(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if f.Opcode == 0x0a { //if execute
			data := f.RawBytes
			idLen := binary.BigEndian.Uint16(data[9:11])
			preparedID := string(data[11:(11 + idLen)])
			if toTargetCassandra {
				keyspace = p.preparedStatementCache[preparedID].Keyspace
			} else {
				keyspace = p.preparedStatementCache[preparedID].Keyspace
			}
		} else {
			if toTargetCassandra {
				keyspace = p.currentTargetCassandraKeyspacePerClient[clientIPAddress]
			} else {
				keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
			}
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = NewQuery(queryType, f, clientIPAddress, GetDestinationCluster(toTargetCassandra), paths, keyspace).UsingTimestamp()

	return q, nil
}

// Create select query. Selects only go to the origin cassandra cluster, so no toTargetCassandra flag is needed here.
func (p *CloudgateProxy) createReadQuery(fromClause string, f *Frame, clientIPAddress string,
	paths []string) (*Query, error) {
	var q *Query

	keyspace := extractKeyspace(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if f.Opcode == 0x0a { //if execute
			data := f.RawBytes
			idLen := binary.BigEndian.Uint16(data[9:11])
			preparedID := string(data[11:(11 + idLen)])
			keyspace = p.preparedStatementCache[preparedID].Keyspace
		} else {
			keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = NewQuery(SELECT, f, clientIPAddress, ORIGIN_CASSANDRA, paths, keyspace).UsingTimestamp()

	return q, nil
}

func (p *CloudgateProxy) createBatchQuery(f *Frame, clientIPAddress string, paths []string, toTargetCassandra bool) (*Query, error) {
	var q *Query
	var keyspace string

	for _, path := range paths {
		fields := strings.Split(path, "/")
		keyspace = extractKeyspace(fields[3])
		if keyspace == "" {
			if toTargetCassandra {
				keyspace = p.currentTargetCassandraKeyspacePerClient[clientIPAddress]
			} else {
				keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
			}
			if keyspace == "" {
				return q, fmt.Errorf("invalid keyspace for batch query (%s, %d)", clientIPAddress, f.Stream)
			}
		}
	}

	q = NewQuery(BATCH, f, clientIPAddress, GetDestinationCluster(toTargetCassandra), paths, keyspace).UsingTimestamp()
	return q, nil
}

// Given a FROM argument, extract the table name
// ex: table, keyspace.table, keyspace.table;, keyspace.table(, etc..
// TODO currently parsing keyspace and table, but table is probably not necessary so it should be taken out
// TODO note that table name is NOT returned
func extractKeyspace(fromClause string) string {
	fromClause = strings.TrimSuffix(fromClause, ";")

	keyspace := ""
	tableName := fromClause

	// Separate keyspace & tableName if together
	if i := strings.IndexRune(fromClause, '.'); i != -1 {
		keyspace = fromClause[:i]
		tableName = fromClause[i+1:]
	}

	// Remove column names if part of an INSERT query: ex: TABLE(col, col)
	if i := strings.IndexRune(tableName, '('); i != -1 {
		tableName = tableName[:i]
	}

	// Make keyspace and tablename lowercase if necessary.
	// Otherwise, leave case as-is but trim quotations marks
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = strings.TrimPrefix(keyspace, "\"")
		keyspace = strings.TrimSuffix(keyspace, "\"")
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	if strings.HasPrefix(tableName, "\"") && strings.HasSuffix(tableName, "\"") {
		tableName = strings.TrimPrefix(tableName, "\"")
		tableName = strings.TrimSuffix(tableName, "\"")
	} else {
		tableName = strings.ToLower(tableName)
	}

	return keyspace
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
