package zdmproxy

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	log "github.com/sirupsen/logrus"
)

type PreparedStatementCache struct {
	cache *simplelru.LRU[string, PreparedData] // Map containing the prepared queries (raw bytes) keyed on prepareId
	index map[string]string                    // Map that can be used as an index to look up origin prepareIds by target prepareId

	interceptedCache *simplelru.LRU[string, PreparedData] // Map containing the prepared queries for intercepted requests

	lock *sync.RWMutex
}

func NewPreparedStatementCache(maxSize int) (*PreparedStatementCache, error) {
	indexMap := make(map[string]string)

	cache, err := simplelru.NewLRU[string, PreparedData](maxSize, func(key string, value PreparedData) {
		// this is called by LRU.Add() so we already have a lock here
		delete(indexMap, string(value.GetTargetPreparedId()))
	})
	if err != nil {
		return nil, fmt.Errorf("error initializing the PreparedStatementCache cache map: %v", err)
	}

	interceptedCache, err := simplelru.NewLRU[string, PreparedData](maxSize, nil)
	if err != nil {
		return nil, fmt.Errorf("error initializing the PreparedStatementCache interceptedCache map: %v", err)
	}

	return &PreparedStatementCache{
		cache:            cache,
		index:            indexMap,
		interceptedCache: interceptedCache,
		lock:             &sync.RWMutex{},
	}, nil
}

func (psc PreparedStatementCache) GetPreparedStatementCacheSize() float64 {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	cacheLen := psc.cache.Len()
	interceptedCacheLen := psc.interceptedCache.Len()

	log.Debugf("PS Cache Size: %v, PS Intercepted Size: %v, PS Index Size: %v.",
		cacheLen, interceptedCacheLen, len(psc.index))

	return float64(cacheLen + interceptedCacheLen)
}

func (psc *PreparedStatementCache) Store(
	originPreparedResult *message.PreparedResult, targetPreparedResult *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo) {

	originPrepareIdStr := string(originPreparedResult.PreparedQueryId)
	targetPrepareIdStr := string(targetPreparedResult.PreparedQueryId)
	psc.lock.Lock()
	defer psc.lock.Unlock()

	psc.cache.Add(originPrepareIdStr, NewPreparedData(originPreparedResult, targetPreparedResult, prepareRequestInfo))
	psc.index[targetPrepareIdStr] = originPrepareIdStr

	log.Debugf("Storing PS cache entry: {OriginPreparedId=%v, TargetPreparedId: %v, RequestInfo: %v}",
		hex.EncodeToString(originPreparedResult.PreparedQueryId), hex.EncodeToString(targetPreparedResult.PreparedQueryId), prepareRequestInfo)
}

func (psc *PreparedStatementCache) StoreIntercepted(preparedResult *message.PreparedResult, prepareRequestInfo *PrepareRequestInfo) {
	prepareIdStr := string(preparedResult.PreparedQueryId)
	psc.lock.Lock()
	defer psc.lock.Unlock()

	preparedData := NewPreparedData(preparedResult, preparedResult, prepareRequestInfo)
	psc.interceptedCache.Add(prepareIdStr, preparedData)

	log.Debugf("Storing intercepted PS cache entry: {PreparedId=%v, RequestInfo: %v}",
		hex.EncodeToString(preparedResult.PreparedQueryId), prepareRequestInfo)
}

func (psc *PreparedStatementCache) Get(originPreparedId []byte) (PreparedData, bool) {
	psc.lock.Lock()
	defer psc.lock.Unlock()
	data, ok := psc.cache.Get(string(originPreparedId))
	if ok {
		return data, true
	}

	data, ok = psc.interceptedCache.Get(string(originPreparedId))
	if ok {
		return data, true
	}

	return nil, false
}

func (psc *PreparedStatementCache) GetByTargetPreparedId(targetPreparedId []byte) (PreparedData, bool) {
	psc.lock.Lock()
	defer psc.lock.Unlock()

	originPreparedId, ok := psc.index[string(targetPreparedId)]
	if !ok {
		// Don't bother attempting a lookup on the intercepted cache because this method should only be used to handle UNPREPARED responses
		return nil, false
	}

	data, ok := psc.cache.Get(originPreparedId)
	if !ok {
		log.Errorf("Could not get prepared data by target id even though there is an entry on the index map. "+
			"This is most likely a bug. OriginPreparedId = %v, TargetPreparedId = %v", originPreparedId, targetPreparedId)
		return nil, false
	}

	return data, true
}

type PreparedData interface {
	GetOriginPreparedId() []byte
	GetTargetPreparedId() []byte
	GetPrepareRequestInfo() *PrepareRequestInfo
	GetOriginVariablesMetadata() *message.VariablesMetadata
	GetTargetVariablesMetadata() *message.VariablesMetadata
}

type preparedDataImpl struct {
	originPreparedId        []byte
	targetPreparedId        []byte
	prepareRequestInfo      *PrepareRequestInfo
	originVariablesMetadata *message.VariablesMetadata
	targetVariablesMetadata *message.VariablesMetadata
}

func NewPreparedData(
	originPreparedResult *message.PreparedResult, targetPreparedResult *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo) PreparedData {
	return &preparedDataImpl{
		originPreparedId:        originPreparedResult.PreparedQueryId,
		targetPreparedId:        targetPreparedResult.PreparedQueryId,
		prepareRequestInfo:      prepareRequestInfo,
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

func (recv *preparedDataImpl) GetPrepareRequestInfo() *PrepareRequestInfo {
	return recv.prepareRequestInfo
}

func (recv *preparedDataImpl) GetOriginVariablesMetadata() *message.VariablesMetadata {
	return recv.originVariablesMetadata
}

func (recv *preparedDataImpl) GetTargetVariablesMetadata() *message.VariablesMetadata {
	return recv.targetVariablesMetadata
}

func (recv *preparedDataImpl) String() string {
	return fmt.Sprintf("PreparedData={OriginPreparedId=%s, TargetPreparedId=%s, PrepareRequestInfo=%v}",
		hex.EncodeToString(recv.originPreparedId), hex.EncodeToString(recv.targetPreparedId), recv.prepareRequestInfo)
}
