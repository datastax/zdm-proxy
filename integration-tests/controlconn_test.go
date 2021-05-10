package integration_tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetHosts(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(true, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	checkHostsFunc := func(t *testing.T, cc *cloudgateproxy.ControlConn, cluster *simulacron.Cluster) {
		clusterName := cc.GetClusterName()
		require.Equal(t, cluster.Name, clusterName)

		hosts, err := cc.GetHosts()
		require.Nil(t, err)
		require.Equal(t, 3, len(hosts))
		nodesByAddress := make(map[string]*simulacron.Node, 3)
		for _, n := range cluster.Datacenters[0].Nodes {
			nodesByAddress[n.Address] = n
		}

		for _, h := range hosts {
			hostId, err := uuid.FromBytes(h.HostId[:])
			require.Nil(t, err)
			require.NotNil(t, hostId)
			require.NotEqual(t, uuid.Nil, hostId)
			endpt := fmt.Sprintf("%s:%d", h.Address, h.Port)
			_, addressInMap := nodesByAddress[endpt]
			require.True(t, addressInMap, fmt.Sprintf("%s does not match a node address in %v", endpt, nodesByAddress))
			require.Equal(t, 9042, h.Port)
			require.Equal(t, "rack1", h.Rack)
			require.Equal(t, env.DseVersion, h.DseVersion)
			require.Equal(t, env.CassandraVersion, h.ReleaseVersion)
			require.Equal(t, "dc1", h.Datacenter)
			require.NotNil(t, h.Tokens)
			require.Equal(t, 1, len(h.Tokens))
		}
	}

	checkHostsFunc(t, testSetup.Proxy.GetOriginControlConn(), testSetup.Origin)
	checkHostsFunc(t, testSetup.Proxy.GetTargetControlConn(), testSetup.Target)
}
