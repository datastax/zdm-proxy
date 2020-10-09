package cloudgateproxy

import (
	"encoding/binary"
	"errors"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type forwardDecision string

const (
	forwardToOrigin = forwardDecision("origin")
	forwardToBoth   = forwardDecision("both")
	forwardToNone   = forwardDecision("none")
)

func inspectFrame(f *Frame, psCache *PreparedStatementCache, mh metrics.IMetricsHandler) (forwardDecision, error) {

	switch f.Opcode {

	case OpCodeQuery:
		query, err := readLongString(f.Body)
		if err != nil {
			return forwardToNone, err
		}
		forwardDecision := inspectCqlQuery(query)
		return forwardDecision, nil

	case OpCodePrepare:
		query, err := readLongString(f.Body)
		if err != nil {
			return forwardToNone, err
		}
		forwardDecision := inspectCqlQuery(query)
		psCache.trackStatementToBePrepared(f.StreamId, forwardDecision == forwardToBoth)
		return forwardDecision, nil

	case OpCodeExecute:
		preparedId, err := readShortBytes(f.Body)
		if err != nil {
			return forwardToNone, err
		}
		log.Debugf("Execute with prepared-id = '%s'", preparedId)
		if stmtInfo, ok := psCache.retrieveStmtInfoFromCache(string(preparedId)); ok {
			// The R/W flag was set in the cache when handling the corresponding PREPARE request
			if stmtInfo.IsWriteStatement {
				return forwardToBoth, nil
			} else {
				return forwardToOrigin, nil
			}
		} else {
			log.Warnf("No cached entry for prepared-id = '%s'", preparedId)
			_ = mh.IncrementCountByOne(metrics.PSCacheMissCount)
			// TODO handle cache miss here! Generate an UNPREPARED response and send straight back to the client?
			return forwardToBoth, nil
		}

	case OpCodeRegister:
		// TODO handle REGISTER messages
	}
	return forwardToBoth, nil
}

// Reads a "long string" from the slice. A long string is defined as an [int] n,
// followed by n bytes representing an UTF-8 string.
func readLongString(data []byte) (string, error) {
	capacity := len(data)
	if capacity < 4 {
		return "", errors.New("not enough bytes to read a long string")
	}
	queryLen := int(binary.BigEndian.Uint32(data[0:4]))
	if capacity < 4+queryLen {
		return "", errors.New("not enough bytes to read a long string")
	}
	longString := string(data[4 : 4+queryLen])
	return longString, nil
}

// Reads a "short bytes" from the slice. A short bytes is defined as a [short] n,
// followed by n bytes if n >= 0.
func readShortBytes(data []byte) ([]byte, error) {
	capacity := cap(data)
	if capacity < 2 {
		return nil, errors.New("not enough bytes to read a short bytes")
	}
	idLen := int(binary.BigEndian.Uint16(data[0:2]))
	if capacity < 2+idLen {
		return nil, errors.New("not enough bytes to read a short bytes")
	}
	shortBytes := data[2 : 2+idLen]
	return shortBytes, nil
}

var commentsRegExp = regexp.MustCompile("(?s)//.*?\n|--.*?\n|/\\*.*?\\*/")

func inspectCqlQuery(query string) forwardDecision {
	query = commentsRegExp.ReplaceAllString(query, "") // remove comments
	query = strings.TrimSpace(query)
	// TODO detect system.local and system.peers
	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		return forwardToOrigin
	} else {
		return forwardToBoth
	}
}
