package cloudgateproxy

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"sync/atomic"
	"time"
)

var singletonGenerator *timeUuidGeneratorImpl
var timeUuidGeneratorFactoryMu = &sync.RWMutex{}
var zeroId [6]byte

type timeUuidGeneratorImpl struct {
	nodeID   [6]byte
	clockSeq uint32
	lastTime int64
}

type TimeUuidGenerator interface {
	GetTimeUuid() uuid.UUID
}

func GetDefaultTimeUuidGenerator() (*timeUuidGeneratorImpl, error) {
	timeUuidGeneratorFactoryMu.RLock()
	generator := singletonGenerator
	timeUuidGeneratorFactoryMu.RUnlock()

	if generator != nil {
		return generator, nil
	}

	timeUuidGeneratorFactoryMu.Lock()
	defer timeUuidGeneratorFactoryMu.Unlock()

	if singletonGenerator != nil {
		return singletonGenerator, nil
	}

	// skip trying to use mac addresses, just generate a random node id
	var newNodeId [6]byte
	_, err := rand.Read(newNodeId[:])
	if err != nil {
		return nil, fmt.Errorf("could not generate node id for timeuuid generation: %w", err)
	}
	newNodeId[0] = newNodeId[0] | 0x01 // multicast bit, check RFC4122

	randomClockSeqSlice := make([]byte, 2)
	_, err = rand.Read(randomClockSeqSlice)
	if err != nil {
		return nil, fmt.Errorf("could not generate clock sequence: %w", err)
	}

	singletonGenerator = &timeUuidGeneratorImpl{
		nodeID:   newNodeId,
		clockSeq: 0,
	}

	atomic.StoreUint32(&singletonGenerator.clockSeq, uint32(binary.BigEndian.Uint16(randomClockSeqSlice)))
	return singletonGenerator, nil
}

var gregorianCalendarTime = time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC)

func (recv *timeUuidGeneratorImpl) GetTimeUuid() uuid.UUID {
	now, clockSeq := recv.getTimeAndClockSeq()
	return newTimeUuid(now, clockSeq, recv.nodeID)
}

func newTimeUuid(now int64, clockSeq uint16, nodeId [6]byte) uuid.UUID {
	var uuid uuid.UUID
	timeLow := uint32(now & 0xffffffff)
	timeMid := uint16((now >> 32) & 0xffff)
	timeHi := uint16((now >> 48) & 0x0fff)

	binary.BigEndian.PutUint32(uuid[0:], timeLow)
	binary.BigEndian.PutUint16(uuid[4:], timeMid)
	binary.BigEndian.PutUint16(uuid[6:], timeHi)
	binary.BigEndian.PutUint16(uuid[8:], clockSeq)
	copy(uuid[10:], nodeId[:])

	uuid[6] &= 0x0F // clear version
	uuid[6] |= 0x10 // set version to 1 (time based uuid)
	uuid[8] &= 0x3F // clear variant
	uuid[8] |= 0x80 // set to IETF variant

	return uuid
}

func (recv *timeUuidGeneratorImpl) getTimeAndClockSeq() (int64, uint16) {
	clockSeq := uint16(atomic.AddUint32(&recv.clockSeq, 1))
	return recv.getTime(time.Now().UTC()), clockSeq
}

func (recv *timeUuidGeneratorImpl) getTime(nowUtc time.Time) int64 {
	nowSeconds := nowUtc.Unix()
	nowNanoseconds := nowUtc.Nanosecond()
	gregorianSeconds := gregorianCalendarTime.Unix()
	gregorianNanoseconds := gregorianCalendarTime.Nanosecond()

	// doing this calculation in pure nanoseconds (time.Duration base unit) and dividing by 100 at the end would result
	// in an overflow so obtain the count of 100-nanosecond intervals right away
	timestampSeconds := time.Duration(nowSeconds-gregorianSeconds) * (time.Second / 100)

	timestampNanoseconds := (time.Duration(nowNanoseconds-gregorianNanoseconds) * time.Nanosecond) / 100
	return (timestampSeconds + timestampNanoseconds).Nanoseconds()
}