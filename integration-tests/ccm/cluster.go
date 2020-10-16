package ccm

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/env"
)

type Cluster struct {
	name                string
	version             string
	initialContactPoint string
	isDse               bool

	startNodeIndex int
	session        *gocql.Session
}

func newCluster(name string, version string, isDse bool, startNodeIndex int) *Cluster {
	return &Cluster{
		name,
		version,
		fmt.Sprintf("127.0.0.%d", startNodeIndex),
		isDse,
		startNodeIndex,
		nil,
	}
}

func GetNewCluster(id uint64, startNodeIndex int, numberOfNodes int, start bool) (*Cluster, error) {
	name := fmt.Sprintf("test_cluster%d", id)
	cluster := newCluster(name, env.ServerVersion, env.IsDse, startNodeIndex)
	err := cluster.Create(numberOfNodes, start)
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

func (ccmCluster *Cluster) GetInitialContactPoint() string {
	return ccmCluster.initialContactPoint
}

func (ccmCluster *Cluster) GetVersion() string {
	return ccmCluster.version
}

func (ccmCluster *Cluster) GetId() string {
	return ccmCluster.name
}

func (ccmCluster *Cluster) GetSession() *gocql.Session {
	return ccmCluster.session
}

func (ccmCluster *Cluster) Create(numberOfNodes int, start bool) error {
	_, err := Create(ccmCluster.name, ccmCluster.version, ccmCluster.isDse)

	if err != nil {
		Remove(ccmCluster.name)
		return err
	}

	for i := 0; i < numberOfNodes; i++ {
		nodeIndex := ccmCluster.startNodeIndex + i
		_, err = Add(
			true,
			fmt.Sprintf("127.0.0.%d", nodeIndex),
			2000+nodeIndex*100,
			7000+nodeIndex*100,
			fmt.Sprintf("node%d", nodeIndex))

		if err != nil {
			Remove(ccmCluster.name)
			return err
		}
	}

	if start {
		_, err = Start()

		if err != nil {
			Remove(ccmCluster.name)
			return err
		}

		gocqlCluster := gocql.NewCluster(ccmCluster.initialContactPoint)
		ccmCluster.session, err = gocqlCluster.CreateSession()

		if err != nil {
			Remove(ccmCluster.name)
			return err
		}
	}

	return nil
}

func (ccmCluster *Cluster) UpdateConf(yamlChanges... string) error {
	err := ccmCluster.SwitchToThis()
	if err != nil {
		return err
	}

	_, err = UpdateConf(yamlChanges...)
	return err
}

func (ccmCluster *Cluster) Start(jvmArgs... string) error {
	err := ccmCluster.SwitchToThis()
	if err != nil {
		return err
	}
	_, err = Start(jvmArgs...)
	return err
}

func (ccmCluster *Cluster) SwitchToThis() error {
	_, err := Switch(ccmCluster.name)
	return err
}

func (ccmCluster *Cluster) Remove() error {
	if ccmCluster.session != nil {
		ccmCluster.session.Close()
	}

	_, err := Remove(ccmCluster.name)
	return err
}
