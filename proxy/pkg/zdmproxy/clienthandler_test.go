package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMaxStreamIds(t *testing.T) {
	type args struct {
		originProtoVer       primitive.ProtocolVersion
		targetProtoVer       primitive.ProtocolVersion
		config               *config.Config
		expectedMaxStreamIds int
	}
	tests := []struct {
		name                 string
		args                 args
		expectedMaxStreamIds int
	}{
		{"OriginV3_TargetV3_DefaultConfig", args{originProtoVer: primitive.ProtocolVersion3, targetProtoVer: primitive.ProtocolVersion3, config: &config.Config{ProxyMaxStreamIds: 2048}}, 2048},
		{"OriginV3_TargetV4_DefaultConfig", args{originProtoVer: primitive.ProtocolVersion3, targetProtoVer: primitive.ProtocolVersion4, config: &config.Config{ProxyMaxStreamIds: 2048}}, 2048},
		{"OriginV3_TargetV4_LowerConfig", args{originProtoVer: primitive.ProtocolVersion3, targetProtoVer: primitive.ProtocolVersion4, config: &config.Config{ProxyMaxStreamIds: 1024}}, 1024},
		{"OriginV2_TargetV3_DefaultConfig", args{originProtoVer: primitive.ProtocolVersion2, targetProtoVer: primitive.ProtocolVersion3, config: &config.Config{ProxyMaxStreamIds: 2048}}, 127},
		{"OriginV2_TargetV2_DefaultConfig", args{originProtoVer: primitive.ProtocolVersion2, targetProtoVer: primitive.ProtocolVersion2, config: &config.Config{ProxyMaxStreamIds: 2048}}, 127},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := maxStreamIds(tt.args.originProtoVer, tt.args.targetProtoVer, tt.args.config)
			require.Equal(t, tt.expectedMaxStreamIds, ids)
		})
	}
}
