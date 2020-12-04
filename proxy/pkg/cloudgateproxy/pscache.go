package cloudgateproxy

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

type preparedStatementInfo struct {
	forwardDecision forwardDecision
}

type PreparedStatementCache struct {

	// Map containing the statement to be prepared and whether it is a read or a write by streamID
	// This is kind of transient: it only contains statements that are being prepared at the moment.
	// Once the response to the prepare request is processed, the statement is removed from this map
	statementsBeingPrepared map[int16]preparedStatementInfo
	// Map containing the prepared queries (raw bytes) keyed on prepareId
	cache map[string]preparedStatementInfo
	lock  *sync.RWMutex
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		statementsBeingPrepared: make(map[int16]preparedStatementInfo),
		cache:                   make(map[string]preparedStatementInfo),
		lock:                    &sync.RWMutex{},
	}
}

func (psc PreparedStatementCache) GetPreparedStatementCacheSize() float64{
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return float64(len(psc.cache))
}

func (psc *PreparedStatementCache) trackStatementToBePrepared(streamId int16, forwardDecision forwardDecision) {
	// add the statement info for this query to the transient map of statements to be prepared
	stmtInfo := preparedStatementInfo{forwardDecision}
	psc.lock.Lock()
	psc.statementsBeingPrepared[streamId] = stmtInfo
	psc.lock.Unlock()
}

func (psc *PreparedStatementCache) cachePreparedId(streamId int16, preparedId []byte) {
	log.Tracef("In cachePreparedId")
	log.Tracef("PreparedID: %s for stream %d", preparedId, streamId)
	psc.lock.Lock()
	log.Tracef("cachePreparedId: lock acquired")
	// move the information about this statement into the cache
	psc.cache[string(preparedId)] = psc.statementsBeingPrepared[streamId]
	log.Tracef("PSInfo set in map for PreparedID: %s", preparedId)
	// remove it from the temporary map
	delete(psc.statementsBeingPrepared, streamId)
	log.Tracef("cachePreparedId: removing statement info from transient map")
	psc.lock.Unlock()
	log.Tracef("cachePreparedId: lock released")
}

func (psc *PreparedStatementCache) retrieveStmtInfoFromCache(preparedId []byte) (preparedStatementInfo, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()
	stmtInfo, ok := psc.cache[string(preparedId)]
	return stmtInfo, ok
}
