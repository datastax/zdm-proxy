package simulacron

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"strings"
)

type Cluster struct {
	*baseSimulacron
	initialContactPoint string
	version             string
	session             *gocql.Session
	datacenters         []*Datacenter
}

type Datacenter struct {
	*baseSimulacron
	nodes []*Node
}

type Node struct {
	*baseSimulacron
	address string
}

type baseSimulacron struct {
	process *Process
	id      int
}

func (process *Process) newCluster(data *ClusterData) (*Cluster, error) {
	dcs := make([]*Datacenter, len(data.Datacenters))
	for i := 0; i < len(dcs); i++ {
		dcs[i] = newDatacenter(process, data.Datacenters[i])
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

	return &Cluster{
		initialContactPoint: contactPoint,
		version:             env.ServerVersion,
		baseSimulacron:      &baseSimulacron{id: data.Id, process: process},
		session:             session,
		datacenters:         dcs,
	}, nil
}

func newNode(process *Process, data *NodeData) *Node {
	return &Node{baseSimulacron: &baseSimulacron{id: data.Id, process: process}, address: data.Address}
}

func newDatacenter(process *Process, data *DatacenterData) *Datacenter {
	nodes := make([]*Node, len(data.Nodes))
	for i := 0; i < len(nodes); i++ {
		nodes[i] = newNode(process, data.Nodes[i])
	}
	return &Datacenter{
		baseSimulacron: &baseSimulacron{id: data.Id, process: process},
		nodes:          nodes,
	}
}

func (baseSimulacron *baseSimulacron) GetId() string {
	return fmt.Sprintf("%d", baseSimulacron.id)
}

func GetNewCluster(numberOfNodes int) (*Cluster, error) {
	process, err := GetOrCreateGlobalSimulacronProcess()

	if err != nil {
		return nil, err
	}

	cluster, createErr := process.Create(numberOfNodes)

	if createErr != nil {
		return nil, createErr
	}

	return cluster, nil
}

func (instance *Cluster) Remove() error {
	if instance.session != nil {
		instance.session.Close()
	}

	return instance.process.Remove(instance.GetId())
}

func (instance *Cluster) GetInitialContactPoint() string {
	return instance.initialContactPoint
}

func (instance *Cluster) GetVersion() string {
	return instance.version
}

func (instance *Cluster) GetSession() *gocql.Session {
	return instance.session
}

func (baseSimulacron *baseSimulacron) Prime(then Then) error {
	_, err := baseSimulacron.process.execHttp("POST", baseSimulacron.getPath("prime"), then.render())
	return err
}

func (baseSimulacron *baseSimulacron) ClearPrimes() error {
	_, err := baseSimulacron.process.execHttp("DELETE", baseSimulacron.getPath("prime"), nil)
	return err
}

func (baseSimulacron *baseSimulacron) getPath(endpoint string) string {
	return "/" + endpoint + "/" + baseSimulacron.GetId()
}
