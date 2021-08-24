package cloudgateproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"sync"
)

type TypeCodecManager interface {
	GetOrCreate(version primitive.ProtocolVersion) *GenericTypeCodec
}

type typeCodecManagerImpl struct {
	rwLock *sync.RWMutex
	codecs map[primitive.ProtocolVersion]*GenericTypeCodec
}

func NewTypeCodecManager() TypeCodecManager {
	return &typeCodecManagerImpl{
		rwLock: &sync.RWMutex{},
		codecs: map[primitive.ProtocolVersion]*GenericTypeCodec{},
	}
}

func (recv typeCodecManagerImpl) GetOrCreate(version primitive.ProtocolVersion) *GenericTypeCodec {
	recv.rwLock.RLock()
	codec, exists := recv.codecs[version]
	recv.rwLock.RUnlock()

	if exists {
		return codec
	}

	recv.rwLock.Lock()
	defer recv.rwLock.Unlock()

	codec, exists = recv.codecs[version]
	if exists {
		return codec
	}

	newCodec := NewDefaultGenericTypeCodec(version)
	recv.codecs[version] = newCodec
	return newCodec
}


func buildDefaultTypeCodecMap() map[datatype.DataType]datatype.Codec {
	commonCodecs := map[datatype.DataType]datatype.Codec{
		datatype.Ascii:    &datatype.AsciiCodec{},
		datatype.Bigint:   &datatype.BigintCodec{},
		datatype.Blob:     &datatype.BlobCodec{},
		datatype.Boolean:  &datatype.BooleanCodec{},
		datatype.Counter:  &datatype.CounterCodec{},
		datatype.Decimal:  &datatype.DecimalCodec{},
		datatype.Double:   &datatype.DoubleCodec{},
		datatype.Float:    &datatype.FloatCodec{},
		datatype.Inet:     &datatype.InetCodec{},
		datatype.Int:      &datatype.IntCodec{},
		datatype.Smallint: &datatype.SmallintCodec{},
		datatype.Text:     &datatype.TextCodec{},
		datatype.Varchar:  &datatype.VarcharCodec{},
		datatype.Timeuuid: &datatype.TimeuuidCodec{},
		datatype.Tinyint:  &datatype.TinyintCodec{},
		datatype.Uuid:     &datatype.UuidCodec{},
		datatype.Varint:   &datatype.VarintCodec{},
	}
	return commonCodecs
}

func NewDefaultGenericTypeCodec(version primitive.ProtocolVersion) *GenericTypeCodec {
	return &GenericTypeCodec{
		version: version,
		codecs:  buildDefaultTypeCodecMap(),
	}
}

type GenericTypeCodec struct {
	version primitive.ProtocolVersion
	codecs  map[datatype.DataType]datatype.Codec
}

func (recv *GenericTypeCodec) Encode(dt datatype.DataType, val interface{}) (encoded []byte, err error) {
	codec, err := recv.createCodecFromDataType(dt)
	if err != nil {
		return nil, err
	}

	return codec.Encode(val, recv.version)
}

func (recv *GenericTypeCodec) Decode(dt datatype.DataType, encoded []byte) (decoded interface{}, err error) {
	codec, err := recv.createCodecFromDataType(dt)
	if err != nil {
		return nil, err
	}

	return codec.Decode(encoded, recv.version)
}

func (recv *GenericTypeCodec) createCodecFromDataType(dt datatype.DataType) (datatype.Codec, error) {
	switch dt.GetDataTypeCode() {
	case primitive.DataTypeCodeList:
		listType := dt.(datatype.ListType)
		elemCodec, err := recv.createCodecFromDataType(listType.GetElementType())
		if err != nil {
			return nil, err
		}
		return datatype.NewListCodec(elemCodec), nil
	case primitive.DataTypeCodeSet:
		setType := dt.(datatype.SetType)
		elemCodec, err := recv.createCodecFromDataType(setType.GetElementType())
		if err != nil {
			return nil, err
		}
		return datatype.NewSetCodec(elemCodec), nil
	case primitive.DataTypeCodeMap:
		mapType := dt.(datatype.MapType)
		keyCodec, err := recv.createCodecFromDataType(mapType.GetKeyType())
		if err != nil {
			return nil, err
		}
		valueCodec, err := recv.createCodecFromDataType(mapType.GetValueType())
		if err != nil {
			return nil, err
		}
		return datatype.NewMapCodec(keyCodec, valueCodec), nil
	case primitive.DataTypeCodeCustom, primitive.DataTypeCodeUdt:
		return &datatype.NilDecoderCodec{}, nil
	default:
		codec, ok := recv.codecs[dt]
		if !ok {
			return nil, fmt.Errorf("could not find codec for DataType %v", dt)
		}

		return codec, nil
	}
}