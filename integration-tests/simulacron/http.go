package simulacron

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

type ClusterData struct {
	Id          int               `json:"id"`
	Datacenters []*DatacenterData `json:"data_centers"`
}

type DatacenterData struct {
	Id    int         `json:"id"`
	Nodes []*NodeData `json:"nodes"`
}

type NodeData struct {
	Id          int      `json:"id"`
	Address     string   `json:"address"`
	Connections []string `json:"connections"`
}

const createUrl = "/cluster?data_centers=%s&cassandra_version=%s&dse_version=%s&name=%s&activity_log=%s&num_tokens=%d"

func (process *Process) Create(startSession bool, numberOfNodes int) (*Cluster, error) {
	name := "test_" + uuid.New().String()
	resp, err := process.execHttp(
		"POST",
		fmt.Sprintf(createUrl, strconv.FormatInt(int64(numberOfNodes), 10), env.CassandraVersion, env.DseVersion, name, "true", 1),
		nil)

	if err != nil {
		return nil, err
	}

	var clusterData ClusterData
	json.Unmarshal(resp, &clusterData)
	return process.newCluster(startSession, &clusterData, name)
}

func (process *Process) Remove(id string) error {
	_, err := process.execHttp(
		"DELETE",
		"/cluster/"+id,
		nil)

	return err
}

func (process *Process) execHttp(method string, url string, body interface{}) ([]byte, error) {
	var requestBody io.Reader

	if body == nil {
		requestBody = nil
	} else {
		bodyBytes, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return nil, marshalErr
		}
		requestBody = bytes.NewBuffer(bodyBytes)
	}

	req, err := http.NewRequest(method, process.baseUrl+url, requestBody)
	if err != nil {
		return nil, err
	}

	if requestBody != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	req.Header.Add("Accept", "application/json")

	resp, respErr := httpClient.Do(req)

	if respErr != nil {
		return nil, respErr
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return nil, errors.New("unexpected status code: " + strconv.FormatInt(int64(resp.StatusCode), 10))
	}

	bytes, readErr := ioutil.ReadAll(resp.Body)

	if readErr != nil {
		return nil, err
	}

	return bytes, nil
}
