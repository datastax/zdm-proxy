package cloudgateproxy

import (
	"encoding/binary"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// Bit within the flags byte of a query denoting whether the query includes a timestamp
	timestampBit = 0x20
)

//func createWriteQuery(queryType Type, f *Frame) *Query {
//	return NewQuery(queryType, f).UsingTimestamp()
//}
//
//func createReadQuery(f *Frame) *Query {
//	return NewQuery(SELECT, f).UsingTimestamp()
//}
//
//func createBatchQuery(f *Frame) *Query {
//	return NewQuery(BATCH, f).UsingTimestamp()
//}

// TODO should the proxy be doing this?? either it is client-driven or it is cluster-driven...
// UsingTimestamp adds a timestamp to a query, if one is not already present.
// Supports BATCH, UPDATE, and INSERT queries.
func (f *Frame) UsingTimestamp() *Frame {
	flagsByte := f.getFlagsByte()
	if flagsByte == -1 {
		log.Errorf("opcode %d not supported for UsingTimestamp() on query %d", f.Opcode, f.StreamId)
		return f
	}

	flags := f.RawBytes[flagsByte]

	// Statement already includes timestamp
	if flags&timestampBit == timestampBit {
		return f
	}

	// Set the timestamp bit to 1
	f.RawBytes[flagsByte] = flags | 0x20

	// Add timestamp to end of query
	timestamp := make([]byte, 8)
	now := uint64(time.Now().UnixNano() / 1000000)
	binary.BigEndian.PutUint64(timestamp, now)
	f.RawBytes = append(f.RawBytes, timestamp...)

	// Update length of body if not BATCH statement
	if f.Opcode != OpCodeBatch {
		bodyLen := binary.BigEndian.Uint32(f.RawBytes[5:9]) + 8
		binary.BigEndian.PutUint32(f.RawBytes[5:9], bodyLen)
	}
	return f
}

func (f *Frame) getFlagsByte() int {
	switch f.Opcode {
	case OpCodeQuery:
		queryLen := binary.BigEndian.Uint32(f.RawBytes[9:13])
		return 15 + int(queryLen)
	case OpCodeExecute:
		queryLen := binary.BigEndian.Uint16(f.RawBytes[9:11])
		return 13 + int(queryLen)
	case OpCodeBatch:
		return flagsByteFromBatch(f.RawBytes)
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
			offset = readPastBatchValues(frame, offset)
		} else if kind == 1 {
			// prepared query id
			idLen := int(binary.BigEndian.Uint16(frame[offset+1 : offset+3]))
			offset = offset + 3 + idLen
			offset = readPastBatchValues(frame, offset)
		}
	}
	return offset + 2
}

// Taken from
// https://github.com/cilium/cilium/blob/2bc1fdeb97331761241f2e4b3fb88ad524a0681b/proxylib/cassandra/cassandraparser.go
// Advances offset to account for potential bound variables in the <query>
func readPastBatchValues(data []byte, initialOffset int) int {
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
