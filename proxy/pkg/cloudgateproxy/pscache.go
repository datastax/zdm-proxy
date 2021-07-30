package cloudgateproxy

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
)

type PreparedStatementCache struct {
	// Map containing the prepared queries (raw bytes) keyed on prepareId
	cache map[string]PreparedData
	lock  *sync.RWMutex
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		cache:                   make(map[string]PreparedData),
		lock:                    &sync.RWMutex{},
	}
}

func (psc PreparedStatementCache) GetPreparedStatementCacheSize() float64{
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return float64(len(psc.cache))
}

func (psc *PreparedStatementCache) Store(
	originPreparedId []byte, targetPreparedId []byte, preparedStmtInfo *PreparedStatementInfo) {
	log.Tracef("PreparedID: %s, TargetPreparedID: %s", base64.StdEncoding.EncodeToString(originPreparedId), hex.EncodeToString(targetPreparedId))

	psc.lock.Lock()
	defer psc.lock.Unlock()

	psc.cache[string(originPreparedId)] = NewPreparedData(targetPreparedId, preparedStmtInfo)

	log.Tracef("PSInfo set in map for OriginPreparedID: %s", hex.EncodeToString(originPreparedId))
}

func (psc *PreparedStatementCache) Get(originPreparedId []byte) (PreparedData, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()
	data, ok := psc.cache[string(originPreparedId)]
	return data, ok
}

type PreparedData interface {
	GetTargetPreparedId() []byte
	GetPreparedStatementInfo() *PreparedStatementInfo
}

type preparedDataImpl struct {
	targetPreparedId []byte
	stmtInfo         *PreparedStatementInfo
}

func NewPreparedData(targetPreparedId []byte, preparedStmtInfo *PreparedStatementInfo) PreparedData {
	return &preparedDataImpl{
		targetPreparedId: targetPreparedId,
		stmtInfo:         preparedStmtInfo,
	}
}

func (recv *preparedDataImpl) GetTargetPreparedId() []byte {
	return recv.targetPreparedId
}

func (recv *preparedDataImpl) GetPreparedStatementInfo() *PreparedStatementInfo {
	return recv.stmtInfo
}

func (recv *preparedDataImpl) String() string {
	return fmt.Sprintf("PreparedData={TargetPreparedId=%s, PreparedStatementInfo=%v}",
		hex.EncodeToString(recv.targetPreparedId), recv.stmtInfo)
}