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

func TestPreparedStatementCache_Store(t *testing.T) {

	tests := []struct {
		name                           string
		numElementsToAdd               int
		elementSuffixesToAccess        []int
		expectedCacheSize              int
		expectedElementSuffixesInCache []int
	}{
		{
			name:                           "insert less elements than capacity, nothing accessed, nothing evicted",
			numElementsToAdd:               9,
			elementSuffixesToAccess:        []int{},
			expectedCacheSize:              9,
			expectedElementSuffixesInCache: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:                           "insert as many elements as capacity, nothing accessed, nothing evicted",
			numElementsToAdd:               10,
			elementSuffixesToAccess:        []int{},
			expectedCacheSize:              10,
			expectedElementSuffixesInCache: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:                           "insert more elements than capacity, nothing accessed, overflowing oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{},
			expectedCacheSize:              MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, only recent ones accessed, overflowing oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{5, 7, 9},
			expectedCacheSize:              MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, overflowing oldest ones accessed, non-accessed oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{0, 2},
			expectedCacheSize:              MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{0, 2, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, overflowing oldest and recent ones accessed, non-accessed oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{0, 2, 3, 8},
			expectedCacheSize:              MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{0, 2, 3, 6, 7, 8, 9, 10, 11, 12},
		},
	}

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
					psCache.Store(originPreparedResult, targetPreparedResult, nil)
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
					psCache.Store(originPreparedResult, targetPreparedResult, nil)
				}

				// access the specified elements
				for _, elementSuffix := range test.elementSuffixesToAccess {
					// access the specified elements to make them recently used
					foundInOriginMap := checkIfElementIsInOriginMap(psCache, elementSuffix)
					require.True(tt, foundInOriginMap, "element could not be found in origin map", elementSuffix)
					foundInTargetMap := checkIfElementIsInTargetMap(psCache, elementSuffix)
					require.True(tt, foundInTargetMap, "element could not be found in target map", elementSuffix)
				}

				// add more elements
				for i := MaxPSCacheSizeForTests; i < test.numElementsToAdd; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.Store(originPreparedResult, targetPreparedResult, nil)
				}
			}

			require.Equal(tt, test.expectedCacheSize, psCache.cache.Len())
			require.Equal(tt, test.expectedCacheSize, psCache.index.Len())
			require.Equal(tt, 0, psCache.interceptedCache.Len())
			require.Equal(tt, float64(test.expectedCacheSize), psCache.GetPreparedStatementCacheSize())

			for _, elementSuffix := range test.expectedElementSuffixesInCache {
				foundInOriginMap := checkIfElementIsInOriginMap(psCache, elementSuffix)
				require.True(tt, foundInOriginMap, "element could not be found in origin map", elementSuffix)
				foundInTargetMap := checkIfElementIsInTargetMap(psCache, elementSuffix)
				require.True(tt, foundInTargetMap, "element could not be found in target map", elementSuffix)
			}

		})
	}

}

func checkIfElementIsInOriginMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	originId := fmt.Sprint(OriginIdPrefix, elementSuffix)
	_, foundOriginId := psCache.Get([]byte(originId))
	return foundOriginId
}

func checkIfElementIsInTargetMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	targetId := fmt.Sprint(TargetIdPrefix, elementSuffix)
	_, foundTargetId := psCache.GetByTargetPreparedId([]byte(targetId))
	return foundTargetId
}

func TestPreparedStatementCache_StoreIntercepted(t *testing.T) {

}

func TestPreparedStatementCache_Get(t *testing.T) {

}

func TestPreparedStatementCache_GetByTargetPreparedId(t *testing.T) {

}

func TestPreparedStatementCache_GetPreparedStatementCacheSize(t *testing.T) {

}
