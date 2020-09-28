package setup

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"sync"
	"time"
)

type TestCluster interface {
	GetInitialContactPoint() string
	GetVersion() string
	GetId() string
	GetSession() *gocql.Session
	Remove() error
}

const (
	originNodes = 1
	targetNodes = 1
)

var mux = &sync.Mutex{}
var r = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

var createdGlobalClusters = false

var globalCcmClusterOrigin *ccm.Cluster
var globalCcmClusterTarget *ccm.Cluster

var globalSimulacronClusterOrigin *simulacron.SimulacronCluster
var globalSimulacronClusterTarget *simulacron.SimulacronCluster

var globalOriginCluster TestCluster
var globalTargetCluster TestCluster

func GetGlobalTestClusterOrigin() (TestCluster, error) {
	if createdGlobalClusters {
		return globalOriginCluster, nil
	}

	mux.Lock()
	defer mux.Unlock()
	if createdGlobalClusters {
		return globalOriginCluster, nil
	}

	err := createClusters()

	if err != nil {
		return nil, err
	}

	return globalOriginCluster, nil
}

func GetGlobalTestClusterTarget() (TestCluster, error) {
	if createdGlobalClusters {
		return globalTargetCluster, nil
	}

	mux.Lock()
	defer mux.Unlock()
	if createdGlobalClusters {
		return globalTargetCluster, nil
	}

	err := createClusters()

	if err != nil {
		return nil, err
	}

	return globalTargetCluster, nil
}

func createClusters() error {
	// assuming we have the lock already

	firstClusterId := r.Uint64() % (math.MaxUint64 - 1)

	var err error
	if env.UseCcmGlobal {
		firstClusterName := fmt.Sprintf("test_cluster%d", firstClusterId)
		globalCcmClusterOrigin = ccm.NewCluster(firstClusterName, env.ServerVersion, env.IsDse, 1)
		err = globalCcmClusterOrigin.Create(originNodes)
		globalOriginCluster = globalCcmClusterOrigin
	} else {
		globalSimulacronClusterOrigin, err = simulacron.GetNewCluster(originNodes)
		globalOriginCluster = globalSimulacronClusterOrigin
	}

	if err != nil {
		return err
	}

	if env.UseCcmGlobal {
		secondClusterId := firstClusterId + 1
		secondClusterName := fmt.Sprintf("test_cluster%d", secondClusterId)
		globalCcmClusterTarget = ccm.NewCluster(secondClusterName, env.ServerVersion, env.IsDse, 10)
		err = globalCcmClusterTarget.Create(targetNodes)
		globalTargetCluster = globalCcmClusterTarget
	} else {
		globalSimulacronClusterTarget, err = simulacron.GetNewCluster(targetNodes)
		globalTargetCluster = globalSimulacronClusterTarget
	}

	if err != nil {
		return err
	}

	createdGlobalClusters = true

	return nil
}

func CleanUpClusters() {
	if !createdGlobalClusters {
		return
	}

	if env.UseCcmGlobal {
		globalCcmClusterTarget.SwitchToThis()
		ccm.RemoveCurrent()
		globalCcmClusterOrigin.SwitchToThis()
		ccm.RemoveCurrent()
	} else {
		err := globalSimulacronClusterOrigin.Remove()
		if err != nil {
			log.Warn("failed to remove simulacron cluster", globalSimulacronClusterOrigin.GetId(), ":", err)
		}

		err = globalSimulacronClusterTarget.Remove()
		if err != nil {
			log.Warn("failed to remove simulacron cluster", globalSimulacronClusterTarget.GetId(), ":", err)
		}

		process, err := simulacron.GetGlobalSimulacronProcess()
		if err != nil {
			log.Error("unable to get simulacron process", err)
		}

		process.Cancel()
	}
}
