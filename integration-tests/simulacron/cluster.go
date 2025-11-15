package simulacron

import (
	"encoding/json"
	"fmt"
	"github.com/apache/cassandra-gocql-driver/v2"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"net"
	"strings"
)

type Cluster struct {
	*baseSimulacron
	InitialContactPoint string
	Version             string
	Session             *gocql.Session
	Datacenters         []*Datacenter
	Name                string
}

type Datacenter struct {
	*baseSimulacron
	Nodes []*Node
}

type Node struct {
	*baseSimulacron
	Address string
}

type baseSimulacron struct {
	process *Process
	id      string
}

func (process *Process) newCluster(startSession bool, data *ClusterData, name string) (*Cluster, error) {
	dcs := make([]*Datacenter, len(data.Datacenters))
	for i := 0; i < len(dcs); i++ {
		dcs[i] = newDatacenter(process, data.Datacenters[i], data.Id)
	}
	var contactPoint string
	var session *gocql.Session
	var err error
	if len(dcs) <= 0 || len(dcs[0].Nodes) <= 0 {
		contactPoint = ""
	} else {
		contactPoint = strings.Split(dcs[0].Nodes[0].Address, ":")[0]
		if startSession {
			cl := gocql.NewCluster(contactPoint)
			session, err = cl.CreateSession()
			if err != nil {
				return nil, err
			}
		}
	}

	return &Cluster{
		InitialContactPoint: contactPoint,
		Version:             env.ServerVersion,
		baseSimulacron:      &baseSimulacron{id: fmt.Sprintf("%d", data.Id), process: process},
		Session:             session,
		Datacenters:         dcs,
		Name:                name,
	}, nil
}

func newNode(process *Process, data *NodeData, dcId int, clusterId int) *Node {
	return &Node{baseSimulacron: &baseSimulacron{id: fmt.Sprintf("%d/%d/%d", clusterId, dcId, data.Id), process: process}, Address: data.Address}
}

func newDatacenter(process *Process, data *DatacenterData, clusterId int) *Datacenter {
	nodes := make([]*Node, len(data.Nodes))
	for i := 0; i < len(nodes); i++ {
		nodes[i] = newNode(process, data.Nodes[i], data.Id, clusterId)
	}
	return &Datacenter{
		baseSimulacron: &baseSimulacron{id: fmt.Sprintf("%d/%d", clusterId, data.Id), process: process},
		Nodes:          nodes,
	}
}

func (baseSimulacron *baseSimulacron) GetId() string {
	return baseSimulacron.id
}

func GetNewCluster(startSession bool, numberOfNodes int, version *ClusterVersion) (*Cluster, error) {
	process, err := GetOrCreateGlobalSimulacronProcess()

	if err != nil {
		return nil, err
	}

	cluster, createErr := process.Create(startSession, numberOfNodes, version)

	if createErr != nil {
		return nil, createErr
	}

	return cluster, nil
}

func (instance *Cluster) Remove() error {
	if instance.Session != nil {
		instance.Session.Close()
	}

	return instance.process.Remove(instance.GetId())
}

func (instance *Cluster) GetInitialContactPoint() string {
	return instance.InitialContactPoint
}

func (instance *Cluster) GetVersion() string {
	return instance.Version
}

func (instance *Cluster) GetSession() *gocql.Session {
	return instance.Session
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
