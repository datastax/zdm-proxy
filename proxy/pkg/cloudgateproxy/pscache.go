package cloudgateproxy

import (
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	log "github.com/sirupsen/logrus"
	"sync"
)

type PreparedStatementCache struct {
	cache map[string]PreparedData // Map containing the prepared queries (raw bytes) keyed on prepareId
	index map[string]string // Map that can be used as an index to look up origin prepareIds by target prepareId
	lock  *sync.RWMutex
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		cache: make(map[string]PreparedData),
		index: make(map[string]string),
		lock:  &sync.RWMutex{},
	}
}

func (psc PreparedStatementCache) GetPreparedStatementCacheSize() float64{
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return float64(len(psc.cache))
}

func (psc *PreparedStatementCache) Store(
	originPreparedResult *message.PreparedResult, targetPreparedResult *message.PreparedResult,
	preparedStmtInfo *PreparedStatementInfo) {

	originPrepareIdStr := string(originPreparedResult.PreparedQueryId)
	targetPrepareIdStr := string(targetPreparedResult.PreparedQueryId)
	psc.lock.Lock()
	defer psc.lock.Unlock()

	psc.cache[originPrepareIdStr] = NewPreparedData(originPreparedResult, targetPreparedResult, preparedStmtInfo)
	psc.index[targetPrepareIdStr] = originPrepareIdStr

	log.Debugf("Storing PS cache entry: {OriginPreparedId=%v, TargetPreparedId: %v, StatementInfo: %v}",
		hex.EncodeToString(originPreparedResult.PreparedQueryId), hex.EncodeToString(targetPreparedResult.PreparedQueryId), preparedStmtInfo)
}

func (psc *PreparedStatementCache) Get(originPreparedId []byte) (PreparedData, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()
	data, ok := psc.cache[string(originPreparedId)]
	return data, ok
}

func (psc *PreparedStatementCache) GetByTargetPreparedId(targetPreparedId []byte) (PreparedData, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	originPreparedId, ok := psc.index[string(targetPreparedId)]
	if !ok {
		return nil, false
	}

	data, ok := psc.cache[originPreparedId]
	if !ok {
		log.Errorf("Could not get prepared data by target id even though there is an entry on the index map. " +
			"This is most likely a bug. OriginPreparedId = %v, TargetPreparedId = %v", originPreparedId, targetPreparedId)
		return nil, false
	}

	return data, true
}

type PreparedData interface {
	GetOriginPreparedId() []byte
	GetTargetPreparedId() []byte
	GetPreparedStatementInfo() *PreparedStatementInfo
	GetOriginVariablesMetadata() *message.VariablesMetadata
	GetTargetVariablesMetadata() *message.VariablesMetadata
}

type preparedDataImpl struct {
	originPreparedId        []byte
	targetPreparedId        []byte
	stmtInfo                *PreparedStatementInfo
	originVariablesMetadata *message.VariablesMetadata
	targetVariablesMetadata *message.VariablesMetadata
}

func NewPreparedData(originPreparedResult *message.PreparedResult, targetPreparedResult *message.PreparedResult,
	preparedStmtInfo *PreparedStatementInfo) PreparedData {
	return &preparedDataImpl{
		originPreparedId:        originPreparedResult.PreparedQueryId,
		targetPreparedId:        targetPreparedResult.PreparedQueryId,
		stmtInfo:                preparedStmtInfo,
		originVariablesMetadata: originPreparedResult.VariablesMetadata,
		targetVariablesMetadata: targetPreparedResult.VariablesMetadata,
	}
}

func (recv *preparedDataImpl) GetOriginPreparedId() []byte {
	return recv.originPreparedId
}

func (recv *preparedDataImpl) GetTargetPreparedId() []byte {
	return recv.targetPreparedId
}

func (recv *preparedDataImpl) GetPreparedStatementInfo() *PreparedStatementInfo {
	return recv.stmtInfo
}

func (recv *preparedDataImpl) GetOriginVariablesMetadata() *message.VariablesMetadata {
	return recv.originVariablesMetadata
}

func (recv *preparedDataImpl) GetTargetVariablesMetadata() *message.VariablesMetadata {
	return recv.targetVariablesMetadata
}

func (recv *preparedDataImpl) String() string {
	return fmt.Sprintf("PreparedData={OriginPreparedId=%s, TargetPreparedId=%s, PreparedStatementInfo=%v}",
		hex.EncodeToString(recv.originPreparedId), hex.EncodeToString(recv.targetPreparedId), recv.stmtInfo)
}