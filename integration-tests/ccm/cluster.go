package ccm

import (
	"fmt"
	"github.com/gocql/gocql"
)

type Cluster struct {
	name string
	version string
	initialContactPoint string
	isDse bool

	startNodeIndex int
	session *gocql.Session
}

func NewCluster(name string, version string, isDse bool, startNodeIndex int) *Cluster {
	return &Cluster{
		name,
		version,
		fmt.Sprintf("127.0.0.%d", startNodeIndex),
		isDse,
		startNodeIndex,
		nil,
	}
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

func (ccmCluster *Cluster) Create(numberOfNodes int) error {
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
			2000 + nodeIndex*100,
			7000 + nodeIndex*100,
			fmt.Sprintf("node%d", nodeIndex))

		if err != nil {
			Remove(ccmCluster.name)
			return err
		}
	}

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

	return nil
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