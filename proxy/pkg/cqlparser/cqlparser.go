package cqlparser

import (
	"encoding/binary"
	"errors"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	UnknownPreparedQueryPath = "/unknown-prepared-query"
)

var opcodeMap = map[byte]string{
	0x00: "error",
	0x01: "startup",
	0x02: "ready",
	0x03: "authenticate",
	0x05: "options",
	0x06: "supported",
	0x07: "query",
	0x08: "result",
	0x09: "prepare",
	0x0A: "execute",
	0x0B: "register",
	0x0C: "event",
	0x0D: "batch",
	0x0E: "auth_challenge",
	0x0F: "auth_response",
	0x10: "auth_success",
}

type PreparedQueries struct {
	// Stores prepared query string while waiting for 'prepared' reply from server with prepared id
	// Replies are associated via stream-id
	PreparedQueryPathByStreamID map[uint16]string

	// Stores query string based on prepared-id
	// Allows us to view query at time of execute command
	PreparedQueryPathByPreparedID map[string]string
}

// Taken with small modifications from
// https://github.com/cilium/cilium/blob/2bc1fdeb97331761241f2e4b3fb88ad524a0681b/proxylib/cassandra/cassandraparser.go
func CassandraParseRequest(p *PreparedQueries, data []byte) ([]string, error) {
	opcode := data[4]
	path := opcodeMap[opcode]

	// parse query string from query/prepare/batch requests

	// NOTE: parsing only prepare statements and passing all execute
	// statements requires that we 'invalidate' all execute statements
	// anytime policy changes, to ensure that no execute statements are
	// allowed that correspond to prepared queries that would no longer
	// be valid.   A better option might be to cache all prepared queries,
	// mapping the execution ID to allow/deny each time policy is changed.
	if opcode == 0x07 || opcode == 0x09 {
		// query || prepare
		queryLen := binary.BigEndian.Uint32(data[9:13])
		endIndex := 13 + queryLen
		query := string(data[13:endIndex])
		action, table := parseCassandra(query)

		if action == "" {
			return nil, errors.New("invalid frame type")
		}

		path = "/" + path + "/" + action + "/" + table
		if opcode == 0x09 {
			// stash 'path' for this prepared query based on stream id
			// rewrite 'opcode' portion of the path to be 'execute' rather than 'prepare'
			streamID := binary.BigEndian.Uint16(data[2:4])
			log.Debugf("Prepare query path '%s' with stream-id %d", path, streamID)
			p.PreparedQueryPathByStreamID[streamID] = strings.Replace(path, "prepare", "execute", 1)
		}
		return []string{path}, nil
	} else if opcode == 0x0d {
		// batch

		numQueries := binary.BigEndian.Uint16(data[10:12])
		paths := make([]string, numQueries)
		log.Debugf("batch query count = %d", numQueries)
		offset := 12
		for i := 0; i < int(numQueries); i++ {
			kind := data[offset]
			if kind == 0 {
				// full query string
				queryLen := int(binary.BigEndian.Uint32(data[offset+1 : offset+5]))

				query := string(data[offset+5 : offset+5+queryLen])
				action, table := parseCassandra(query)

				if action == "" {
					return nil, errors.New("invalid frame type")
				}
				path = "/" + path + "/" + action + "/" + table
				paths[i] = path
				path = "batch" // reset for next item
				offset = offset + 5 + queryLen
				offset = ReadPastBatchValues(data, offset)
			} else if kind == 1 {
				// prepared query id

				idLen := int(binary.BigEndian.Uint16(data[offset+1 : offset+3]))
				preparedID := string(data[offset+3 : (offset + 3 + idLen)])
				log.Debugf("Batch entry with prepared-id = '%s'", preparedID)
				path := p.PreparedQueryPathByPreparedID[preparedID]
				if len(path) > 0 {
					paths[i] = path
				} else {
					log.Warnf("No cached entry for prepared-id = '%s' in batch", preparedID)

					return []string{UnknownPreparedQueryPath}, nil
				}
				offset = offset + 3 + idLen

				offset = ReadPastBatchValues(data, offset)
			} else {
				log.Errorf("unexpected value of 'kind' in batch query: %d", kind)
				return nil, errors.New("processing batch command failed")
			}
		}
		return paths, nil
	} else if opcode == 0x0a {
		// execute

		// parse out prepared query id, and then look up our
		// cached query path for policy evaluation.
		idLen := binary.BigEndian.Uint16(data[9:11])
		preparedID := string(data[11:(11 + idLen)])
		log.Debugf("Execute with prepared-id = '%s'", preparedID)
		path := p.PreparedQueryPathByPreparedID[preparedID]

		// action, table = parseCassandra (raw bytes body)
		// path = /execute/action/table

		if len(path) == 0 {
			log.Warnf("No cached entry for prepared-id = '%s'", preparedID)

			return []string{UnknownPreparedQueryPath}, nil
		}

		return []string{path}, nil
	} else {
		// other opcode, just return type of opcode

		return []string{"/" + path}, nil
	}
}

// Modified from
// https://github.com/cilium/cilium/blob/2bc1fdeb97331761241f2e4b3fb88ad524a0681b/proxylib/cassandra/cassandraparser.go
// to return lowercase action and as-is table (including quotations and case-sensitivity)
func parseCassandra(query string) (string, string) {
	var action string
	var table string

	query = strings.TrimRight(query, ";")            // remove potential trailing ;
	fields := strings.Fields(strings.ToLower(query)) // handles all whitespace
	originalFields := strings.Fields(query)          // maintains case-sensitiviy for table

	// we currently do not strip comments.  It seems like cqlsh does
	// strip comments, but its not clear if that can be assumed of all clients
	// It should not be possible to "spoof" the 'action' as this is assumed to be
	// the first token (leaving no room for a comment to start), but it could potentially
	// trick this parser into thinking we're accessing table X, when in fact the
	// query accesses table Y, which would obviously be a security vulnerability
	// As a result, we look at each token here, and if any of them match the comment
	// characters for cassandra, we fail parsing.
	for i := 0; i < len(fields); i++ {
		if len(fields[i]) >= 2 &&
			(fields[i][:2] == "--" ||
				fields[i][:2] == "/*" ||
				fields[i][:2] == "//") {

			log.Warnf("Unable to safely parse query with comments '%s'", query)
			return "", ""
		}
	}
	if len(fields) < 2 {
		goto invalidQuery
	}

	action = fields[0]
	switch action {
	case "select", "delete":
		for i := 1; i < len(fields); i++ {
			if fields[i] == "from" {
				table = originalFields[i+1]
			}
		}
		if len(table) == 0 {
			log.Warnf("Unable to parse table name from query '%s'", query)
			return "", ""
		}
	case "insert":
		// INSERT into <table-name>
		if len(fields) < 3 {
			goto invalidQuery
		}
		table = originalFields[2]
	case "update":
		// UPDATE <table-name>
		table = originalFields[1]
	case "use":
		table = originalFields[1]
	case "alter", "create", "drop", "truncate", "list":

		action = strings.Join([]string{action, fields[1]}, "-")
		if fields[1] == "table" || fields[1] == "keyspace" {

			if len(fields) < 3 {
				goto invalidQuery
			}
			table = originalFields[2]
			if strings.ToLower(table) == "if" {
				if action == "create-table" {
					if len(fields) < 6 {
						goto invalidQuery
					}
					// handle optional "IF NOT EXISTS"
					table = originalFields[5]
				} else if action == "drop-table" || action == "drop-keyspace" {
					if len(fields) < 5 {
						goto invalidQuery
					}
					// handle optional "IF EXISTS"
					table = originalFields[4]
				}
			}
		}
		if action == "truncate" && len(fields) == 2 {
			// special case, truncate can just be passed table name
			table = originalFields[1]
		}
		if fields[1] == "materialized" {
			action = action + "-view"
		} else if fields[1] == "custom" {
			action = "create-index"
		}
	default:
		goto invalidQuery
	}

	return action, table

invalidQuery:

	log.Errorf("Unable to parse query: '%s'", query)
	return "", ""
}

// Taken from
// https://github.com/cilium/cilium/blob/2bc1fdeb97331761241f2e4b3fb88ad524a0681b/proxylib/cassandra/cassandraparser.go
// Advances offset to account for potential bound variables in the <query>
func ReadPastBatchValues(data []byte, initialOffset int) int {
	numValues := int(binary.BigEndian.Uint16(data[initialOffset : initialOffset+2]))
	offset := initialOffset + 2
	for i := 0; i < numValues; i++ {
		valueLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		// handle 'null' (-1) and 'not set' (-2) case, where 0 bytes follow
		if valueLen >= 0 {
			offset = offset + 4 + valueLen
		}
	}
	return offset
}
