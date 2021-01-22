package simulacron

import (
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"net"
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

func (process *Process) newCluster(startSession bool, data *ClusterData) (*Cluster, error) {
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
		if startSession {
			cl := gocql.NewCluster(contactPoint)
			session, err = cl.CreateSession()
			if err != nil {
				return nil, err
			}
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

func GetNewCluster(startSession bool, numberOfNodes int) (*Cluster, error) {
	process, err := GetOrCreateGlobalSimulacronProcess()

	if err != nil {
		return nil, err
	}

	cluster, createErr := process.Create(startSession, numberOfNodes)

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

func (baseSimulacron *baseSimulacron) GetConnections() ([]string, error) {
	bytes, err := baseSimulacron.process.execHttp("DELETE", baseSimulacron.getPath("connections"), nil)
	if err != nil {
		return nil, err
	}

	var clusterData ClusterData
	err = json.Unmarshal(bytes, &clusterData)
	if err != nil {
		return nil, err
	}

	var connections []string
	for _, dc := range clusterData.Datacenters {
		for _, node := range dc.Nodes {
			connections = append(connections, node.Connections...)
		}
	}

	return connections, nil
}

func (baseSimulacron *baseSimulacron) DropConnection(endpoint string) error {
	ip, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return err
	}

	_, err = baseSimulacron.process.execHttp(
		"DELETE",
		fmt.Sprintf("%s/%s/%s", baseSimulacron.getPath("connection"), ip, port),
		nil)

	if err != nil {
		return err
	}

	return nil
}

func (baseSimulacron *baseSimulacron) DropAllConnections() error {
	_, err := baseSimulacron.process.execHttp(
		"DELETE",
		baseSimulacron.getPath("connections"),
		nil)

	if err != nil {
		return err
	}

	return nil
}

func (baseSimulacron *baseSimulacron) DisableConnectionListener() error {
	_, err := baseSimulacron.process.execHttp(
		"DELETE",
		fmt.Sprintf("%s?after=%d&type=%s", baseSimulacron.getPath("listener"), 0, "unbind"),
		nil)

	if err != nil {
		return err
	}

	return nil
}

func (baseSimulacron *baseSimulacron) EnableConnectionListener() error {
	_, err := baseSimulacron.process.execHttp(
		"PUT",
		fmt.Sprintf("%s?after=%d&type=%s", baseSimulacron.getPath("listener"), 0, "unbind"),
		nil)

	if err != nil {
		return err
	}

	return nil
}

func (baseSimulacron *baseSimulacron) getPath(endpoint string) string {
	return "/" + endpoint + "/" + baseSimulacron.GetId()
}
