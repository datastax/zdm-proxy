package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestAsyncReadError(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.ReadMode = config.ReadModeDualAsyncOnSecondary
	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	query := "SELECT * FROM test"
	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{
			"abcd": simulacron.DataTypeText,
		}).WithRow(map[string]interface{}{
		"abcd": "123",
	})

	err = testSetup.Origin.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows))
	require.Nil(t, err)

	err = testSetup.Target.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenReadTimeout(gocql.LocalOne, 0, 0, false))
	require.Nil(t, err)

	client := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlClientConn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	require.Nil(t, err)
	defer cqlClientConn.Close()

	queryMsg := &message.Query{
		Query:   query,
		Options: nil,
	}

	rsp, err := cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
	rowsMsg, ok := rsp.Body.Message.(*message.RowsResult)
	require.True(t, ok)
	require.Equal(t, 1, len(rowsMsg.Metadata.Columns))
	require.Equal(t, "abcd", rowsMsg.Metadata.Columns[0].Name)
}

func TestAsyncReadHighLatency(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.ReadMode = config.ReadModeDualAsyncOnSecondary
	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	query := "SELECT * FROM test"
	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{
			"abcd": simulacron.DataTypeText,
		}).WithRow(map[string]interface{}{
		"abcd": "123",
	})

	err = testSetup.Origin.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows))
	require.Nil(t, err)

	err = testSetup.Target.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenReadTimeout(gocql.LocalOne, 0, 0, false).WithDelay(1 * time.Second))
	require.Nil(t, err)

	client := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlClientConn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	require.Nil(t, err)
	defer cqlClientConn.Close()

	queryMsg := &message.Query{
		Query:   query,
		Options: nil,
	}

	now := time.Now()
	rsp, err := cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
	require.Less(t, time.Now().Sub(now).Milliseconds(), int64(500))
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
	rowsMsg, ok := rsp.Body.Message.(*message.RowsResult)
	require.True(t, ok)
	require.Equal(t, 1, len(rowsMsg.Metadata.Columns))
	require.Equal(t, "abcd", rowsMsg.Metadata.Columns[0].Name)
}

func TestAsyncExhaustedStreamIds(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.ReadMode = config.ReadModeDualAsyncOnSecondary
	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	query := "SELECT * FROM test"
	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{
			"abcd": simulacron.DataTypeText,
		}).WithRow(map[string]interface{}{
		"abcd": "123",
	})

	err = testSetup.Origin.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows))
	require.Nil(t, err)

	err = testSetup.Target.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows).WithDelay(10 * time.Second))
	require.Nil(t, err)

	client := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlClientConn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	require.Nil(t, err)
	defer cqlClientConn.Close()

	queryMsg := &message.Query{
		Query:   query,
		Options: nil,
	}

	now := time.Now()

	oldLevel := log.GetLevel()
	oldZeroLogLevel := zerolog.GlobalLevel()
	log.SetLevel(log.WarnLevel)
	defer log.SetLevel(oldLevel)
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	defer zerolog.SetGlobalLevel(oldZeroLogLevel)

	workers := 10
	totalRequests := 2500 // 2048 stream ids available
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < totalRequests/workers; j++ {
				rsp, err := cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
				assert.Nil(t, err)
				if err != nil {
					continue
				}
				assert.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
				rowsMsg, ok := rsp.Body.Message.(*message.RowsResult)
				assert.True(t, ok)
				if !ok {
					continue
				}
				assert.Equal(t, 1, len(rowsMsg.Metadata.Columns))
				assert.Equal(t, "abcd", rowsMsg.Metadata.Columns[0].Name)
			}
		}()
	}
	wg.Wait()
	require.Less(t, time.Now().Sub(now).Seconds(), float64(5))
}

func TestAsyncReadsRequestTypes(t *testing.T) {

	tests := []*struct {
		name                   string
		msg                    message.Message
		expectedOpCode         primitive.OpCode
		sentPrimary            bool
		sentSecondary          bool
		sentAsync              bool
		primedQuery            simulacron.Then
		prepared               bool
		sentExecuteToPrimary   bool
		sentExecuteToSecondary bool
		sentExecuteToAsync     bool
	}{
		{
			name:           "OPTIONS",
			msg:            &message.Options{},
			expectedOpCode: primitive.OpCodeSupported,
			sentPrimary:    true,
			sentSecondary:  true,
			sentAsync:      true,
			primedQuery:    nil,
			prepared:       false,
		},
		{
			name: "QUERY_USE",
			msg: &message.Query{
				Query: "USE testks",
			},
			expectedOpCode: primitive.OpCodeResult,
			sentPrimary:    true,
			sentSecondary:  true,
			sentAsync:      true,
			primedQuery: simulacron.WhenQuery(
				"USE testks", simulacron.NewWhenQueryOptions()).
				ThenSuccess(),
			prepared: false,
		},
		{
			name: "QUERY_SELECT",
			msg: &message.Query{
				Query: "SELECT * FROM test",
			},
			expectedOpCode: primitive.OpCodeResult,
			sentPrimary:    true,
			sentSecondary:  false,
			sentAsync:      true,
			primedQuery: simulacron.WhenQuery(
				"SELECT * FROM test", simulacron.NewWhenQueryOptions()).
				ThenServerError(simulacron.SyntaxError, "test"),
			prepared: false,
		},
		{
			name: "PREPARE_EXECUTE_SELECT",
			msg: &message.Prepare{
				Query: "SELECT * FROM test",
			},
			expectedOpCode:         primitive.OpCodeResult,
			sentPrimary:            true,
			sentSecondary:          false,
			sentAsync:              true,
			primedQuery:            nil,
			prepared:               true,
			sentExecuteToPrimary:   true,
			sentExecuteToSecondary: false,
			sentExecuteToAsync:     true,
		},
		{
			name: "QUERY_INSERT",
			msg: &message.Query{
				Query: "INSERT INTO test (a, b) VALUES (1, 2)",
			},
			expectedOpCode: primitive.OpCodeResult,
			sentPrimary:    true,
			sentSecondary:  true,
			sentAsync:      false,
			primedQuery:    nil,
			prepared:       false,
		},
		{
			name: "PREPARE_EXECUTE_INSERT",
			msg: &message.Prepare{
				Query: "INSERT INTO test (a, b) VALUES (1, 2)",
			},
			expectedOpCode:         primitive.OpCodeResult,
			sentPrimary:            true,
			sentSecondary:          true,
			sentAsync:              false,
			primedQuery:            nil,
			prepared:               true,
			sentExecuteToPrimary:   true,
			sentExecuteToSecondary: true,
			sentExecuteToAsync:     false,
		},
	}

	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(
		t, false, false, 1, nil)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	testFunc := func(t *testing.T, conf *config.Config) {

		log.SetLevel(log.TraceLevel)
		defer log.SetLevel(log.InfoLevel)
		proxy, err := setup.NewProxyInstanceWithConfig(conf)
		require.Nil(t, err)
		defer proxy.Shutdown()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client := client.NewCqlClient("127.0.0.1:14002", nil)
				cqlClientConn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
				require.Nil(t, err)
				defer cqlClientConn.Close()
				err = testSetup.Origin.DeleteLogs()
				require.Nil(t, err)
				err = testSetup.Target.DeleteLogs()
				require.Nil(t, err)
				f := frame.NewFrame(primitive.ProtocolVersion4, 0, tt.msg)
				rsp, err := cqlClientConn.SendAndReceive(f)
				require.Nil(t, err)
				require.NotNil(t, rsp)
				require.Equal(t, tt.expectedOpCode, rsp.Header.OpCode)
				if tt.prepared {
					preparedMsg := tt.msg.(*message.Prepare)
					require.NotNil(t, preparedMsg)
					preparedResult, ok := rsp.Body.Message.(*message.PreparedResult)
					require.True(t, ok)
					execute := &message.Execute{
						QueryId:          preparedResult.PreparedQueryId,
						ResultMetadataId: preparedResult.ResultMetadataId,
						Options:          nil,
					}
					f = frame.NewFrame(primitive.ProtocolVersion4, 0, execute)
					rsp, err = cqlClientConn.SendAndReceive(f)
					require.Nil(t, err)
					require.NotNil(t, rsp)
					require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
				}

				var simulacronType simulacron.QueryType
				switch tt.msg.GetOpCode() {
				case primitive.OpCodeOptions:
					simulacronType = simulacron.QueryTypeOptions
				case primitive.OpCodeQuery:
					simulacronType = simulacron.QueryTypeQuery
				case primitive.OpCodePrepare:
					simulacronType = simulacron.QueryTypePrepare
				default:
					require.Fail(t, "unexpected request type")
				}

				sentOrigin := 0
				sentTarget := 0
				if conf.PrimaryCluster == config.PrimaryClusterTarget {
					if tt.sentAsync {
						sentOrigin++
					}
					if tt.sentSecondary {
						sentOrigin++
					}
					if tt.sentPrimary {
						sentTarget++
					}
				} else {
					if tt.sentAsync {
						sentTarget++
					}
					if tt.sentSecondary {
						sentTarget++
					}
					if tt.sentPrimary {
						sentOrigin++
					}
				}

				utils.RequireWithRetries(t, func() (err error, fatal bool) {
					originLogs, err := testSetup.Origin.GetLogsByType(simulacronType)
					if err != nil {
						return err, true
					}
					targetLogs, err := testSetup.Target.GetLogsByType(simulacronType)
					if err != nil {
						return err, true
					}
					if sentOrigin != len(originLogs.Datacenters[0].Nodes[0].Queries) {
						return fmt.Errorf("%v expected %v, got %v", simulacronType, sentOrigin, len(originLogs.Datacenters[0].Nodes[0].Queries)), false
					}
					if sentTarget != len(targetLogs.Datacenters[0].Nodes[0].Queries) {
						return fmt.Errorf("%v expected %v, got %v", simulacronType, sentTarget, len(targetLogs.Datacenters[0].Nodes[0].Queries)), false
					}

					if tt.prepared {
						originLogs, err = testSetup.Origin.GetLogsByType(simulacron.QueryTypeExecute)
						if err != nil {
							return err, true
						}
						targetLogs, err = testSetup.Target.GetLogsByType(simulacron.QueryTypeExecute)
						if err != nil {
							return err, true
						}
						sentOrigin = 0
						sentTarget = 0
						if conf.PrimaryCluster == config.PrimaryClusterTarget {
							if tt.sentExecuteToAsync {
								sentOrigin++
							}
							if tt.sentExecuteToSecondary {
								sentOrigin++
							}
							if tt.sentExecuteToPrimary {
								sentTarget++
							}
						} else {
							if tt.sentExecuteToAsync {
								sentTarget++
							}
							if tt.sentExecuteToSecondary {
								sentTarget++
							}
							if tt.sentExecuteToPrimary {
								sentOrigin++
							}
						}
						if sentOrigin != len(originLogs.Datacenters[0].Nodes[0].Queries) {
							return fmt.Errorf("%v expected %v, got %v", simulacron.QueryTypeExecute, sentOrigin, len(originLogs.Datacenters[0].Nodes[0].Queries)), false
						}
						if sentTarget != len(targetLogs.Datacenters[0].Nodes[0].Queries) {
							return fmt.Errorf("%v expected %v, got %v", simulacron.QueryTypeExecute, sentTarget, len(targetLogs.Datacenters[0].Nodes[0].Queries)), false
						}
					}
					return nil, false
				}, 15, 100*time.Millisecond)

			})
		}
	}

	c := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
	c.ReadMode = config.ReadModeDualAsyncOnSecondary

	c.PrimaryCluster = config.PrimaryClusterTarget
	t.Run("ForwardReadsToTarget", func(t *testing.T) {
		testFunc(t, c)
	})

	c.PrimaryCluster = config.PrimaryClusterOrigin
	t.Run("ForwardReadsToOrigin", func(t *testing.T) {
		testFunc(t, c)
	})
}
