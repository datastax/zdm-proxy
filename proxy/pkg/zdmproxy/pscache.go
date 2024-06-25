package zdmproxy

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	log "github.com/sirupsen/logrus"
	"sync"
)

type originPreparedId = string
type targetPreparedId = string
type md5digest = [16]byte

var NilPreparedData PreparedData = &preparedDataImpl{}

type PreparedStatementCache struct {
	cache       map[md5digest]PreparedEntry    // Map containing the prepared queries (raw bytes) keyed on prepareId
	indexTarget map[targetPreparedId]md5digest // Map that can be used as an index to look up client prepareIds by target prepareId
	indexOrigin map[originPreparedId]md5digest // Map that can be used as an index to look up origin prepareIds by target prepareId
	lock        *sync.RWMutex
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		cache:       make(map[md5digest]PreparedEntry),
		indexTarget: make(map[targetPreparedId]md5digest),
		indexOrigin: make(map[originPreparedId]md5digest),
		lock:        &sync.RWMutex{},
	}
}

func (psc *PreparedStatementCache) GetPreparedStatementCacheSize() float64 {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return float64(len(psc.cache))
}

func (psc *PreparedStatementCache) StorePreparedOnBoth(
	originPreparedResult *message.PreparedResult, targetPreparedResult *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo) PreparedEntry {

	originPrepareIdStr := string(originPreparedResult.PreparedQueryId)
	targetPrepareIdStr := string(targetPreparedResult.PreparedQueryId)
	psc.lock.Lock()
	defer psc.lock.Unlock()

	clientPreparedId := psc.computePreparedId(prepareRequestInfo)
	existingEntry, _ := psc.cache[clientPreparedId]
	psc.validateIndexEntry(existingEntry, common.ClusterTypeTarget, clientPreparedId, targetPreparedResult.PreparedQueryId, prepareRequestInfo)
	psc.validateIndexEntry(existingEntry, common.ClusterTypeOrigin, clientPreparedId, originPreparedResult.PreparedQueryId, prepareRequestInfo)

	entry := NewPreparedEntry(clientPreparedId, prepareRequestInfo, NewPreparedData(originPreparedResult, targetPreparedResult))
	psc.cache[clientPreparedId] = entry
	psc.indexOrigin[originPrepareIdStr] = clientPreparedId
	psc.indexTarget[targetPrepareIdStr] = clientPreparedId

	log.Debugf("Storing PS cache entry: %s", entry)
	return entry
}

func (psc *PreparedStatementCache) StorePreparedOnTarget(
	targetPreparedResult *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo) PreparedEntry {

	psc.lock.Lock()
	defer psc.lock.Unlock()

	entry := psc.storeOneClusterOnly(common.ClusterTypeTarget, targetPreparedResult,
		prepareRequestInfo, psc.indexTarget)

	log.Debugf("Storing PS cache entry (target only PS): %s", entry)
	return entry
}

func (psc *PreparedStatementCache) StorePreparedOnOrigin(
	originPreparedResult *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo) PreparedEntry {

	psc.lock.Lock()
	defer psc.lock.Unlock()

	entry := psc.storeOneClusterOnly(common.ClusterTypeOrigin, originPreparedResult,
		prepareRequestInfo, psc.indexOrigin)

	log.Debugf("Storing PS cache entry (origin only PS): %s", entry)
	return entry
}

func (psc *PreparedStatementCache) StoreIntercepted(prepareRequestInfo *PrepareRequestInfo) PreparedEntry {
	psc.lock.Lock()
	defer psc.lock.Unlock()

	clientPreparedId := psc.computePreparedId(prepareRequestInfo)
	existingEntry, _ := psc.cache[clientPreparedId]
	psc.validateIndexEntry(existingEntry, common.ClusterTypeNone, clientPreparedId, nil, prepareRequestInfo)

	entry := NewInterceptedPreparedEntry(clientPreparedId, prepareRequestInfo)
	psc.cache[clientPreparedId] = entry

	log.Debugf("Storing intercepted PS cache entry: %s", entry)
	return entry
}

func (psc *PreparedStatementCache) GetByClientPreparedId(clientPreparedId md5digest) (PreparedEntry, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()
	data, ok := psc.cache[clientPreparedId]
	return data, ok
}

func (psc *PreparedStatementCache) GetByOriginPreparedId(originPreparedId []byte) (PreparedEntry, bool) {
	return psc.getByClusterPreparedId(common.ClusterTypeOrigin, originPreparedId)
}

func (psc *PreparedStatementCache) GetByTargetPreparedId(targetPreparedId []byte) (PreparedEntry, bool) {
	return psc.getByClusterPreparedId(common.ClusterTypeTarget, targetPreparedId)
}

func (psc *PreparedStatementCache) getByClusterPreparedId(cluster common.ClusterType, id []byte) (PreparedEntry, bool) {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	var index map[string]md5digest
	if cluster == common.ClusterTypeOrigin {
		index = psc.indexOrigin
	} else if cluster == common.ClusterTypeTarget {
		index = psc.indexTarget
	} else {
		panic(fmt.Errorf("unexpected cluster type %v", cluster))
	}
	clientPreparedId, ok := index[string(id)]
	if !ok {
		return nil, false
	}

	data, ok := psc.cache[clientPreparedId]
	if !ok {
		log.Errorf("Could not get prepared data by %v id even though there is an entry on the index map. "+
			"This is most likely a bug. ClientPreparedId = %v, %vPreparedId = %v", cluster, hex.EncodeToString(clientPreparedId[:]), cluster, hex.EncodeToString(id))
		return nil, false
	}

	return data, true
}
func (psc *PreparedStatementCache) validateIndexEntry(
	entry PreparedEntry, cluster common.ClusterType, clientPreparedId md5digest, preparedQueryId []byte, prepareRequestInfo *PrepareRequestInfo) {

	var indexMap map[string]md5digest
	if cluster == common.ClusterTypeTarget {
		indexMap = psc.indexTarget
	} else if cluster == common.ClusterTypeOrigin {
		indexMap = psc.indexOrigin
	} // will be None for intercepted queries

	if indexMap != nil && preparedQueryId != nil {
		clusterClientPreparedId, ok := indexMap[string(preparedQueryId)]
		if ok && clusterClientPreparedId != clientPreparedId {
			log.Warnf("Prepared Statement '%v' computes a ZDM prepare id of '%v' but %v index maps this statement "+
				"(%v id = '%v') to another ZDM prepare id '%v'. Removing ZDM id '%v' from PS Cache.",
				prepareRequestInfo.GetQuery(), hex.EncodeToString(clientPreparedId[:]), cluster, cluster, hex.EncodeToString(preparedQueryId),
				hex.EncodeToString(clusterClientPreparedId[:]), hex.EncodeToString(clientPreparedId[:]))
			delete(psc.cache, clusterClientPreparedId)
		}
	}

	if entry != nil {
		if !entry.IsIntercepted() {
			data := entry.GetPreparedData()
			targetId := preparedQueryId
			if cluster != common.ClusterTypeTarget {
				targetId = nil
			}
			if data.GetTargetPreparedId() != nil && targetId != nil && !bytes.Equal(data.GetTargetPreparedId(), targetId) {
				log.Warnf("Prepared Statement '%v' retrieved a TARGET ID of '%v' but this ZDM ID '%v' "+
					"already mapped to a different target id '%v'. Removing Target ID '%v' from PS Cache Index.",
					prepareRequestInfo.GetQuery(), hex.EncodeToString(targetId),
					hex.EncodeToString(clientPreparedId[:]), hex.EncodeToString(data.GetTargetPreparedId()),
					hex.EncodeToString(data.GetTargetPreparedId()))
				delete(psc.indexTarget, string(data.GetTargetPreparedId()))
			}

			originId := preparedQueryId
			if cluster != common.ClusterTypeOrigin {
				originId = nil
			}
			if data.GetOriginPreparedId() != nil && originId != nil && !bytes.Equal(data.GetOriginPreparedId(), originId) {
				log.Warnf("Prepared Statement '%v' retrieved a ORIGIN ID of '%v' but this ZDM ID '%v' "+
					"already mapped to a different origin id '%v'. Removing Origin ID '%v' from PS Cache Index.",
					prepareRequestInfo.GetQuery(), hex.EncodeToString(originId),
					hex.EncodeToString(clientPreparedId[:]), hex.EncodeToString(data.GetOriginPreparedId()),
					hex.EncodeToString(data.GetOriginPreparedId()))
				delete(psc.indexOrigin, string(data.GetOriginPreparedId()))
			}
		}
	}
}

func (psc *PreparedStatementCache) storeOneClusterOnly(
	cluster common.ClusterType,
	result *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo,
	indexMap map[string]md5digest) PreparedEntry {

	preparedIdStr := string(result.PreparedQueryId)

	clientPreparedId := psc.computePreparedId(prepareRequestInfo)
	existingEntry, ok := psc.cache[clientPreparedId]
	psc.validateIndexEntry(existingEntry, cluster, clientPreparedId, result.PreparedQueryId, prepareRequestInfo)

	var existingData PreparedData
	if !ok {
		existingData = NilPreparedData
	} else {
		existingData = existingEntry.GetPreparedData()
	}

	entry := NewPreparedEntry(clientPreparedId, prepareRequestInfo, NewSingleClusterPreparedData(existingData, cluster, result))
	psc.cache[clientPreparedId] = entry
	indexMap[preparedIdStr] = clientPreparedId
	return entry
}

func (psc *PreparedStatementCache) computePreparedId(requestInfo *PrepareRequestInfo) md5digest {
	// https://github.com/apache/cassandra/blob/336ad623c163ea3411032d5be26ead1475ebac8d/src/java/org/apache/cassandra/cql3/QueryProcessor.java#L722-L726
	// https://issues.apache.org/jira/browse/CASSANDRA-15252
	if requestInfo.IsFullyQualified() {
		return md5.Sum([]byte(requestInfo.GetQuery()))
	}

	return psc.computePreparedIdWithKeyspace(requestInfo)
}

func (psc *PreparedStatementCache) computePreparedIdWithKeyspace(requestInfo *PrepareRequestInfo) md5digest {
	var text string
	if requestInfo.GetKeyspace() != "" {
		text = requestInfo.GetKeyspace() + requestInfo.GetQuery()
	} else if requestInfo.GetCurrentKeyspace() != "" {
		text = requestInfo.GetCurrentKeyspace() + requestInfo.GetQuery()
	} else {
		text = requestInfo.GetQuery()
	}
	return md5.Sum([]byte(text))
}

// PreparedEntry only contains InterceptedData or PreparedData, never both
type PreparedEntry interface {
	GetClientPreparedId() md5digest
	GetPrepareRequestInfo() *PrepareRequestInfo
	IsIntercepted() bool
	GetPreparedData() PreparedData
	String() string
}

type preparedEntryImpl struct {
	clientPreparedId   md5digest
	prepareRequestInfo *PrepareRequestInfo
	preparedData       PreparedData
}

func NewInterceptedPreparedEntry(clientPreparedId md5digest, prepareRequestInfo *PrepareRequestInfo) PreparedEntry {
	return &preparedEntryImpl{
		clientPreparedId:   clientPreparedId,
		prepareRequestInfo: prepareRequestInfo,
	}
}

func NewPreparedEntry(clientPreparedId md5digest, prepareRequestInfo *PrepareRequestInfo, preparedData PreparedData) PreparedEntry {
	return &preparedEntryImpl{
		clientPreparedId:   clientPreparedId,
		prepareRequestInfo: prepareRequestInfo,
		preparedData:       preparedData,
	}
}

func (recv *preparedEntryImpl) GetClientPreparedId() md5digest {
	return recv.clientPreparedId
}

func (recv *preparedEntryImpl) GetPrepareRequestInfo() *PrepareRequestInfo {
	return recv.prepareRequestInfo
}

func (recv *preparedEntryImpl) IsIntercepted() bool {
	return recv.preparedData == nil
}

func (recv *preparedEntryImpl) GetPreparedData() PreparedData {
	return recv.preparedData
}

func (recv *preparedEntryImpl) String() string {
	return fmt.Sprintf("PreparedEntry={ClientPreparedId=%s, PrepareRequestInfo=%s, PreparedData=%s}",
		hex.EncodeToString(recv.clientPreparedId[:]), recv.GetPrepareRequestInfo().String(), recv.GetPreparedData())
}

type PreparedData interface {
	GetOriginPreparedId() []byte
	GetTargetPreparedId() []byte
	GetOriginVariablesMetadata() *message.VariablesMetadata
	GetTargetVariablesMetadata() *message.VariablesMetadata
	String() string
}

type preparedDataImpl struct {
	originPreparedId        []byte
	targetPreparedId        []byte
	originVariablesMetadata *message.VariablesMetadata
	targetVariablesMetadata *message.VariablesMetadata
}

func NewPreparedData(
	originPreparedResult *message.PreparedResult, targetPreparedResult *message.PreparedResult) PreparedData {
	return &preparedDataImpl{
		originPreparedId:        originPreparedResult.PreparedQueryId,
		targetPreparedId:        targetPreparedResult.PreparedQueryId,
		originVariablesMetadata: originPreparedResult.VariablesMetadata,
		targetVariablesMetadata: targetPreparedResult.VariablesMetadata,
	}
}

func NewSingleClusterPreparedData(
	existingData PreparedData,
	cluster common.ClusterType,
	preparedResult *message.PreparedResult) PreparedData {
	var originId []byte
	var targetId []byte
	var originVariablesMetadata *message.VariablesMetadata
	var targetVariablesMetadata *message.VariablesMetadata
	if cluster == common.ClusterTypeTarget {
		targetId = preparedResult.PreparedQueryId
		targetVariablesMetadata = preparedResult.VariablesMetadata
		originId = existingData.GetOriginPreparedId()
		originVariablesMetadata = existingData.GetOriginVariablesMetadata()
	} else if cluster == common.ClusterTypeOrigin {
		originId = preparedResult.PreparedQueryId
		originVariablesMetadata = preparedResult.VariablesMetadata
		targetId = existingData.GetTargetPreparedId()
		targetVariablesMetadata = existingData.GetTargetVariablesMetadata()
	} else {
		panic(fmt.Errorf("unexpected cluster type %v", cluster))
	}
	return &preparedDataImpl{
		originPreparedId:        originId,
		targetPreparedId:        targetId,
		originVariablesMetadata: originVariablesMetadata,
		targetVariablesMetadata: targetVariablesMetadata,
	}
}

func (recv *preparedDataImpl) GetOriginPreparedId() []byte {
	return recv.originPreparedId
}

func (recv *preparedDataImpl) GetTargetPreparedId() []byte {
	return recv.targetPreparedId
}

func (recv *preparedDataImpl) GetOriginVariablesMetadata() *message.VariablesMetadata {
	return recv.originVariablesMetadata
}

func (recv *preparedDataImpl) GetTargetVariablesMetadata() *message.VariablesMetadata {
	return recv.targetVariablesMetadata
}

func (recv *preparedDataImpl) String() string {
	return fmt.Sprintf("PreparedData={OriginPreparedId=%s, TargetPreparedId=%s}",
		hex.EncodeToString(recv.originPreparedId), hex.EncodeToString(recv.targetPreparedId))
}

func ConvertToMd5Digest(id []byte) md5digest {
	return *(*md5digest)(id)
}
