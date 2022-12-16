package zdmproxy

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestStreamIdMapper(t *testing.T) {
	var mapper = NewStreamIdMapper(2048, nil)
	var syntheticId, _ = mapper.GetNewIdFor(1000)
	var originalId, _ = mapper.ReleaseId(syntheticId)
	require.Equal(t, int16(1000), originalId)
}

func BenchmarkStreamIdMapper(b *testing.B) {
	var mapper = NewStreamIdMapper(2048, nil)
	for i := 0; i < b.N; i++ {
		var originalId = int16(i)
		var syntheticId, _ = mapper.GetNewIdFor(originalId)
		mapper.ReleaseId(syntheticId)
	}
}

func TestConcurrentStreamIdMapper(t *testing.T) {
	var requestCount = 1 << 17
	var concurrency = 20
	var wg = sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		var mapper = NewStreamIdMapper(2048, nil)
		getAndReleaseIds(t, mapper, int16(i), requestCount, &wg)
	}
	wg.Wait()
}

func getAndReleaseIds(t *testing.T, mapper StreamIdMapper, streamId int16, iterations int, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			var syntheticId, err = mapper.GetNewIdFor(streamId)
			if err != nil {
				t.Fatal(err)
			}
			var returnedId, _ = mapper.ReleaseId(syntheticId)
			mapper.ReleaseId(syntheticId)
			require.Equal(t, streamId, returnedId)
		}
	}()
}
