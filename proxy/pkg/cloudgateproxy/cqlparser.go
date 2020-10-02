package cloudgateproxy

import (
	"encoding/binary"
	"errors"
	"regexp"
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

/*  This function takes a request (represented as raw bytes) and parses it.

The raw byte representation of the request is parsed according to its type.
If the request is to prepare or execute a prepared statement, the map of prepared statements is used (this is keyed on prepareId)

Returns list of []string paths in form /opcode/action/table (one path if a simple request, multiple paths if a batch)
 - opcode is "startup", "query", "batch", etc.
 - action is "select", "insert", "update", etc,
 - table is the table as written in the command

Parsing requests is not cluster-specific, even considering prepared statements (as preparedIDs are computed based on the statement only)

This function was taken from
https://github.com/cilium/cilium/blob/2bc1fdeb97331761241f2e4b3fb88ad524a0681b/proxylib/cassandra/cassandraparser.go
with the following modifications: parsing of EXECUTE statements was modified to determine EXECUTE's action
from the bytes of the original PREPARE message associated with it
*/
func CassandraParseRequest(psCache *PreparedStatementCache, data []byte) ([]string, bool, error) {
	opcode := data[4]
	path := opcodeMap[opcode]

	// flag that tracks whether this request consists of write(s) only
	// if it is a single statement, it will be false if the statement is a read
	// if it is a batch, it will be false if the batch only contains reads, and true otherwise (i.e. there is at least a write)
	isWriteRequest := false

	if opcode == 0x07 || opcode == 0x09 {
		// query || prepare
		queryLen := binary.BigEndian.Uint32(data[9:13])
		endIndex := 13 + queryLen
		query := string(data[13:endIndex])
		action, table := parseCassandra(query)

		if action == "" {
			return nil, false, errors.New("invalid frame type")
		}

		isWriteRequest = isWriteAction(action)

		path = "/" + path + "/" + action + "/" + table
		return []string{path}, isWriteRequest, nil
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
					return nil, false, errors.New("invalid frame type for a query while processing batch")
				}

				isWriteRequest = isWriteRequest || isWriteAction(action)

				path = "/" + path + "/" + action + "/" + table
				paths[i] = path
				path = "batch" // reset for next item
				offset = offset + 5 + queryLen
				offset = ReadPastBatchValues(data, offset)
			} else if kind == 1 {
				//  execute an already-prepared statement as part of the batch
				preparedID, idLen := extractPreparedIDFromRawRequest(data, offset+1)

				if stmtInfo, ok := psCache.retrieveStmtInfoFromCache(preparedID); ok {
					paths[i] = "/execute"
					// The R/W flag was set in the cache when handling the corresponding PREPARE request
					isWriteRequest = isWriteRequest || stmtInfo.IsWriteStatement
				} else {
					log.Warnf("No cached entry for prepared-id = '%s'", preparedID)
					// TODO handle cache miss here! Generate an UNPREPARED response and send straight back to the client?
					return []string{UnknownPreparedQueryPath}, isWriteRequest, nil
				}

				offset = offset + 3 + idLen
				offset = ReadPastBatchValues(data, offset)
			} else {
				log.Errorf("unexpected value of 'kind' in batch query: %d", kind)
				return nil, false, errors.New("processing batch command failed")
			}
		}
		return paths, isWriteRequest, nil
	} else if opcode == 0x0a {
		// execute an already-prepared statement
		preparedID, _ := extractPreparedIDFromRawRequest(data, 9)

		if stmtInfo, ok := psCache.retrieveStmtInfoFromCache(preparedID); ok {
			// The R/W flag was set in the cache when handling the corresponding PREPARE request
			return []string{"/execute"}, stmtInfo.IsWriteStatement, nil
		} else {
			log.Warnf("No cached entry for prepared-id = '%s'", preparedID)
			// TODO handle cache miss here! Generate an UNPREPARED response and send straight back to the client?
			return []string{UnknownPreparedQueryPath}, isWriteRequest, nil
		}

	} else {
		// other opcode, just return type of opcode
		return []string{"/" + path}, isWriteRequest, nil
	}
}

//TODO what about other types of actions such as USE?
func isWriteAction(action string) bool {
	switch action {
	case "select":
		return false
	default:
		return true
	}
}

func extractPreparedIDFromRawRequest(rawRequest []byte, startByteIndex int) (string, int) {
	endByteIndex := startByteIndex + 2
	idLen := int(binary.BigEndian.Uint16(rawRequest[startByteIndex:endByteIndex]))
	preparedID := string(rawRequest[endByteIndex:(endByteIndex + idLen)])
	log.Debugf("Execute with prepared-id = '%s'", preparedID)
	return preparedID, idLen
}

// Modified from
// https://github.com/cilium/cilium/blob/2bc1fdeb97331761241f2e4b3fb88ad524a0681b/proxylib/cassandra/cassandraparser.go
// to return lowercase action and as-is table (including quotations and case-sensitivity)
func parseCassandra(query string) (string, string) {
	var action string
	var table string

	query = strings.TrimRight(query, ";") // remove potential trailing ;

	re := regexp.MustCompile("(?s)//.*?\n|/\\*.*?\\*/")
	query = re.ReplaceAllString(query, "") // remove comments

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
			//TODO should this return an error?
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
		if action == "truncate" && len(fields) == 2 {
			// special case, truncate can just be passed table name
			table = originalFields[1]
			break
		} else if action == "truncate" && len(fields) == 3 {
			// special case, truncate can be 'truncate table keyspace.tablename'
			table = originalFields[2]
			break
		}
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
