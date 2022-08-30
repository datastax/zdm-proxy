package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/datacodec"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

var singletonCodec = &GenericTypeCodec{}

func GetDefaultGenericTypeCodec() *GenericTypeCodec {
	return singletonCodec
}

type GenericTypeCodec struct {
}

func (recv *GenericTypeCodec) Encode(
	dt datatype.DataType, val interface{}, version primitive.ProtocolVersion) (encoded []byte, err error) {
	codec, err := datacodec.NewCodec(dt)
	if err != nil {
		return nil, err
	}

	return codec.Encode(val, version)
}

func (recv *GenericTypeCodec) Decode(
	dt datatype.DataType, encoded []byte, version primitive.ProtocolVersion) (decoded interface{}, err error) {
	codec, err := datacodec.NewCodec(dt)
	if err != nil {
		return nil, err
	}

	var out interface{}
	_, err = codec.Decode(encoded, &out, version)
	if err != nil {
		return nil, err
	}

	return out, nil
}