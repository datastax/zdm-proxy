package zdmproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
	"testing"
)

const MaxPSCacheSizeForTests = 10
const OriginIdPrefix = "originId_"
const TargetIdPrefix = "targetId_"
const InterceptedIdPrefix = "interceptedId_"

type CacheMapType string

const (
	CacheMapTypeOrigin = CacheMapType("CACHE-ORIGIN")
	CacheMapTypeTarget = CacheMapType("INDEX-TARGET")
	CacheMapTypeIntercepted  = CacheMapType("INTERCEPTED")
	CacheMapTypeNone = CacheMapType("NONE")
)

/*
*
This test has the sole purpose to verify that all three cache maps ("cache", "index" and "intercepted") honour the configured size limit and behave in an LRU fashion
It does not represent a realistic usage of the PS Cache: elements are added to all three cache maps, which would not normally happen, and the data inserted in it is intentionally dummy
*/
func TestPreparedStatementCache_StoreIntoAllCacheMaps(t *testing.T) {

	tests := []struct {
		name                           string
		numElementsToAdd               int
		elementSuffixesToAccess        []int
		expectedCacheMapSize           int
		expectedElementSuffixesInCache []int
	}{
		{
			name:                           "insert less elements than capacity, nothing accessed, nothing evicted",
			numElementsToAdd:               9,
			elementSuffixesToAccess:        []int{},
			expectedCacheMapSize:           9,
			expectedElementSuffixesInCache: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:                           "insert as many elements as capacity, nothing accessed, nothing evicted",
			numElementsToAdd:               10,
			elementSuffixesToAccess:        []int{},
			expectedCacheMapSize:           10,
			expectedElementSuffixesInCache: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:                           "insert more elements than capacity, nothing accessed, overflowing oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, only recent ones accessed, overflowing oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{5, 7, 9},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, overflowing oldest ones accessed, non-accessed oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{0, 2},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{0, 2, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, overflowing oldest and recent ones accessed, non-accessed oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{0, 2, 3, 8},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{0, 2, 3, 6, 7, 8, 9, 10, 11, 12},
		},
	}

	dummyPrepareRequestInfo := NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false, false), []*term{}, false, "", "")

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			psCache, err := NewPreparedStatementCache(MaxPSCacheSizeForTests)
			require.Nil(tt, err, "Error creating the PSCache", err)

			if test.numElementsToAdd < MaxPSCacheSizeForTests {
				// no overflow or evictions, just insert all elements
				for i := 0; i < test.numElementsToAdd; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.Store(originPreparedResult, targetPreparedResult, dummyPrepareRequestInfo)

					interceptedPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(InterceptedIdPrefix, i)),
					}
					psCache.StoreIntercepted(interceptedPreparedResult, dummyPrepareRequestInfo)
				}
			} else {
				// fill the cache
				for i := 0; i < MaxPSCacheSizeForTests; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.Store(originPreparedResult, targetPreparedResult, dummyPrepareRequestInfo)

					interceptedPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(InterceptedIdPrefix, i)),
					}
					psCache.StoreIntercepted(interceptedPreparedResult, dummyPrepareRequestInfo)
				}

				// access the specified elements
				for _, elementSuffix := range test.elementSuffixesToAccess {
					// access the specified elements to make them recently used
					foundInOriginMap := checkIfElementIsInOriginMap(psCache, elementSuffix)
					require.True(tt, foundInOriginMap, "element could not be found in origin map", elementSuffix)
					foundInTargetMap := checkIfElementIsInTargetMap(psCache, elementSuffix)
					require.True(tt, foundInTargetMap, "element could not be found in target map", elementSuffix)
					foundInInterceptedMap := checkIfElementIsInInterceptedMap(psCache, elementSuffix)
					require.True(tt, foundInInterceptedMap, "element could not be found in intercepted map", elementSuffix)
				}

				// add more elements
				for i := MaxPSCacheSizeForTests; i < test.numElementsToAdd; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.Store(originPreparedResult, targetPreparedResult, dummyPrepareRequestInfo)

					interceptedPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(InterceptedIdPrefix, i)),
					}
					psCache.StoreIntercepted(interceptedPreparedResult, dummyPrepareRequestInfo)
				}
			}

			require.Equal(tt, test.expectedCacheMapSize, psCache.cache.Len())
			require.Equal(tt, test.expectedCacheMapSize, psCache.index.Len())
			require.Equal(tt, test.expectedCacheMapSize, psCache.interceptedCache.Len())
			require.Equal(tt, float64(test.expectedCacheMapSize*2), psCache.GetPreparedStatementCacheSize())

			for _, elementSuffix := range test.expectedElementSuffixesInCache {
				foundInOriginMap := checkIfElementIsInOriginMap(psCache, elementSuffix)
				require.True(tt, foundInOriginMap, "element could not be found in origin map", elementSuffix)
				foundInTargetMap := checkIfElementIsInTargetMap(psCache, elementSuffix)
				require.True(tt, foundInTargetMap, "element could not be found in target map", elementSuffix)
				foundInInterceptedMap := checkIfElementIsInInterceptedMap(psCache, elementSuffix)
				require.True(tt, foundInInterceptedMap, "element could not be found in intercepted map", elementSuffix)
			}

		})
	}

}

func checkIfElementIsInOriginMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	originId := fmt.Sprint(OriginIdPrefix, elementSuffix)
	// not using psCache.Get, which is tested separately
	_, foundOriginId := psCache.cache.Get(originId)
	return foundOriginId
}

func checkIfElementIsInTargetMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	targetId := fmt.Sprint(TargetIdPrefix, elementSuffix)
	// not using psCache.GetByTargetPreparedId, which is tested separately
	_, foundTargetId := psCache.index.Get(targetId)
	return foundTargetId
}

func checkIfElementIsInInterceptedMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	interceptedId := fmt.Sprint(InterceptedIdPrefix, elementSuffix)
	// not using psCache.Get, which is tested separately
	_, foundInterceptedId := psCache.interceptedCache.Get(interceptedId)
	return foundInterceptedId
}

/**
This test focuses on ensuring that Get and GetByTargetPreparedId work correctly.
It inserts elements directly into the cache maps to avoid coupling this test to the logic in the PS Cache's store methods.
It uses dummy, non-realistic data.
 */
func TestPreparedStatementCache_GetFromCache(t *testing.T) {

	tests := []struct {
		name              string
		elementId string
		cacheMapType      CacheMapType
	}{
		{
			name: "Add to origin cache map, found by Get",
			elementId: "someOriginId",
			cacheMapType: CacheMapTypeOrigin,
		},
		{
			name: "Add to target cache map, found by GetByTargetId",
			elementId: "someTargetId",
			cacheMapType: CacheMapTypeTarget,
		},
		{
			name: "Add to intercepted cache map, found by Get",
			elementId: "someInterceptedId",
			cacheMapType: CacheMapTypeTarget,
		},
		{
			name: "Not added, not found",
			elementId: "someElementId",
			cacheMapType: CacheMapTypeNone,
		},
	}

	dummyPreparedResult := &message.PreparedResult{
		PreparedQueryId: []byte("dummy"),
	}
	dummyPreparedData := NewPreparedData(dummyPreparedResult, dummyPreparedResult, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false, false), []*term{}, false, "", ""))

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			psCache, err := NewPreparedStatementCache(MaxPSCacheSizeForTests)
			require.Nil(tt, err, "Error creating the PSCache", err)

			switch test.cacheMapType {
			case CacheMapTypeOrigin:
				psCache.cache.Add(test.elementId, dummyPreparedData)

				_, foundByGet := psCache.Get([]byte(test.elementId))
				require.True(tt, foundByGet)

				_, foundByGetByTargetPreparedId := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.False(tt, foundByGetByTargetPreparedId)
			case CacheMapTypeTarget:
				psCache.index.Add(test.elementId, "origin_" + test.elementId)
				psCache.cache.Add("origin_" + test.elementId, dummyPreparedData)

				_, foundByGet := psCache.Get([]byte(test.elementId))
				require.False(tt, foundByGet)

				_, foundByGetByTargetPreparedId := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.True(tt, foundByGetByTargetPreparedId)
			case CacheMapTypeIntercepted:
				psCache.interceptedCache.Add(test.elementId, dummyPreparedData)

				_, foundByGet := psCache.Get([]byte(test.elementId))
				require.True(tt, foundByGet)

				_, foundByGetByTargetPreparedId := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.False(tt, foundByGetByTargetPreparedId)
			case CacheMapTypeNone:
				_, foundByGet := psCache.Get([]byte(test.elementId))
				require.False(tt, foundByGet)

				_, foundByGetByTargetPreparedId := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.False(tt, foundByGetByTargetPreparedId)
			default:
				t.Fatal("Unknown or missing cache map type")
			}
		})
	}

}
