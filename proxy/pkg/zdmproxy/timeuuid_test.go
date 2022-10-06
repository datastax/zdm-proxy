package zdmproxy

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSingletonTimeUuid(t *testing.T) {
	generator, err := GetDefaultTimeUuidGenerator()
	require.Nil(t, err)

	generator2, err2 := GetDefaultTimeUuidGenerator()
	require.Nil(t, err2)

	require.Same(t, generator, generator2)
}

func TestTimeUuidGenerator(t *testing.T) {
	generator, err := GetDefaultTimeUuidGenerator()
	require.Nil(t, err)

	now := time.Now().UTC().Add(-100 * time.Nanosecond)
	nowPlusTwoSeconds := time.Now().UTC().Add(2 * time.Second)
	timeUuid := generator.GetTimeUuid()

	require.Equal(t, uuid.Version(1), timeUuid.Version())
	require.Equal(t, uuid.RFC4122, timeUuid.Variant())

	timeUnixNano := time.Unix(timeUuid.Time().UnixTime()).UTC().UnixNano()
	require.GreaterOrEqual(t, timeUnixNano, now.UnixNano())
	require.LessOrEqual(t, timeUnixNano, nowPlusTwoSeconds.UnixNano())

	nodeId := timeUuid.NodeID()
	require.NotEqual(t, [6]byte{}, nodeId) // at least multicast bit is set

	clockSeq := timeUuid.ClockSequence()

	secondTimeUuid := generator.GetTimeUuid()
	require.Equal(t, uuid.Version(1), secondTimeUuid.Version())
	require.Equal(t, uuid.RFC4122, secondTimeUuid.Variant())

	secondTimeUnixNano := time.Unix(secondTimeUuid.Time().UnixTime()).UnixNano()
	require.GreaterOrEqual(t, secondTimeUnixNano, timeUnixNano)
	require.LessOrEqual(t, secondTimeUnixNano, nowPlusTwoSeconds.UnixNano())

	secondClockSeq := secondTimeUuid.ClockSequence()
	require.Equal(t, clockSeq+1, secondClockSeq)

	secondNodeId := secondTimeUuid.NodeID()
	require.Equal(t, nodeId, secondNodeId)
}

func TestSpecificTimeUuid(t *testing.T) {
	generator, err := GetDefaultTimeUuidGenerator()
	require.Nil(t, err)

	// generated type 1 uuid with uuid.NewUUID() from google/uuid
	expectedTimeUuidStr := "bf928f8a-4188-11ec-807b-000204060708"
	expectedSeconds := int64(1636481612)
	expectedNanoseconds := int64(705370600)
	expectedClockSequence := uint16(123)
	expectedTime := time.Unix(expectedSeconds, expectedNanoseconds).UTC()
	expectedNodeId := [6]byte{0, 2, 4, 6, 7, 8}

	timeComponent := generator.getTime(expectedTime)
	timeUuid := newTimeUuid(timeComponent, expectedClockSequence, expectedNodeId)

	require.Equal(t, expectedTimeUuidStr, timeUuid.String())
}
