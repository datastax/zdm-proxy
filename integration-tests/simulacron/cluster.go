package simulacron

import (
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"strings"
)

type SimulacronCluster struct {
	initialContactPoint string
	version string
	id string
	session *gocql.Session
	datacenters []*SimulacronDatacenter
	process *SimulacronProcess
}

type SimulacronDatacenter struct {
	id string
	nodes []*SimulacronNode
}

type SimulacronNode struct {
	id string
	address string
}

type SimulacronBase interface {
	GetId() string
}

func (process *SimulacronProcess) newCluster(data *ClusterData) (*SimulacronCluster, error) {
	dcs := make([]*SimulacronDatacenter, len(data.Datacenters))
	for i := 0; i < len(dcs); i++ {
		dcs[i] = newDatacenter(data.Datacenters[i])
	}
	var contactPoint string
	var session *gocql.Session
	var err error
	if len(dcs) <= 0 || len(dcs[0].nodes) <= 0 {
		contactPoint = ""
	} else {
		contactPoint = strings.Split(dcs[0].nodes[0].address, ":")[0]
		session, err = gocql.NewCluster(contactPoint).CreateSession()
		if err != nil {
			return nil, err
		}
	}

	return &SimulacronCluster{
		initialContactPoint: contactPoint,
		version:             env.ServerVersion,
		id:                  data.Id,
		session:             session,
		datacenters:         dcs,
		process:             process,
	}, nil
}

func newNode(data *NodeData) *SimulacronNode {
	return &SimulacronNode{id: data.Id, address: data.Address}
}

func newDatacenter(data *DatacenterData) *SimulacronDatacenter {
	nodes := make([]*SimulacronNode, len(data.Nodes))
	for i := 0; i < len(nodes); i++ {
		nodes[i] = newNode(data.Nodes[i])
	}
	return &SimulacronDatacenter{
		id:    data.Id,
		nodes: nodes,
	}
}

func (instance *SimulacronCluster) GetId() string {
	return instance.id
}

func (instance *SimulacronNode) GetId() string {
	return instance.id
}

func (instance *SimulacronDatacenter) GetId() string {
	return instance.id
}

func GetNewCluster(numberOfNodes int) (*SimulacronCluster, error) {
	process, err := GetGlobalSimulacronProcess()

	if err != nil {
		return nil, err
	}

	cluster, createErr := process.Create(numberOfNodes)

	if createErr != nil {
		return nil, createErr
	}

	return cluster, nil
}

func (instance *SimulacronCluster) Remove() error {
	if instance.session != nil {
		instance.session.Close()
	}

	return instance.process.Remove(instance.id)
}

func (instance *SimulacronCluster) GetInitialContactPoint() string {
	return instance.initialContactPoint
}

func (instance *SimulacronCluster) GetVersion() string {
	return instance.version
}

func (instance *SimulacronCluster) GetSession() *gocql.Session {
	return instance.session
}