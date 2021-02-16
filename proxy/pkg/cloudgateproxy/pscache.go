package cloudgateproxy

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

type PreparedStatementCache struct {
	// Map containing the prepared queries (raw bytes) keyed on prepareId
	cache map[string]*PreparedStatementInfo
	lock  *sync.RWMutex
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		cache:                   make(map[string]*PreparedStatementInfo),
		lock:                    &sync.RWMutex{},
	}
}

func (psc PreparedStatementCache) GetPreparedStatementCacheSize() float64{
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return float64(len(psc.cache))
}

func (psc *PreparedStatementCache) cachePreparedId(preparedId []byte, preparedStmtInfo *PreparedStatementInfo) {
	log.Tracef("PreparedID: %s", preparedId)

	psc.lock.Lock()
	defer psc.lock.Unlock()

	psc.cache[string(preparedId)] = preparedStmtInfo

	log.Tracef("PSInfo set in map for PreparedID: %s", preparedId)
}

func (psc *PreparedStatementCache) retrieveStmtInfoFromCache(preparedId []byte) (*PreparedStatementInfo, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()
	stmtInfo, ok := psc.cache[string(preparedId)]
	return stmtInfo, ok
}
