package cloudgateproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/datacodec"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAddValuesToExecuteFrame_NoReplacedTerms(t *testing.T) {
	generator, err := newTimeUuidGenerator()
	require.Nil(t, err)
	parameterModifier := NewParameterModifier(generator)
	f := frame.NewFrame(primitive.ProtocolVersion4, 1, &message.Execute{
		QueryId:          nil,
		ResultMetadataId: nil,
		Options:          &message.QueryOptions{},
	})
	prepareRequestInfo := NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), []*term{}, false, "", "")
	variablesMetadata := &message.VariablesMetadata{
		PkIndices: nil,
		Columns:   nil,
	}
	fClone := f.Clone()
	replacementTimeUuids := parameterModifier.generateTimeUuids(prepareRequestInfo)
	newMsg, err := parameterModifier.AddValuesToExecuteFrame(fClone, prepareRequestInfo, variablesMetadata, replacementTimeUuids)
	require.Same(t, fClone.Body.Message, newMsg)
	require.NotSame(t, f.Body.Message, newMsg)
	require.Equal(t, f.Body.Message, newMsg)
}

func TestAddValuesToExecuteFrame_InvalidMessageType(t *testing.T) {
	generator, err := newTimeUuidGenerator()
	require.Nil(t, err)
	parameterModifier := NewParameterModifier(generator)
	require.Nil(t, err)
	f := frame.NewFrame(primitive.ProtocolVersion4, 1, &message.Query{
		Query:   "SELECT * FROM asd WHERE a = :param1",
		Options: &message.QueryOptions{},
	})
	prepareRequestInfo := NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), []*term{}, false, "", "")
	variablesMetadata := &message.VariablesMetadata{
		PkIndices: nil,
		Columns:   nil,
	}
	replacementTimeUuids := parameterModifier.generateTimeUuids(prepareRequestInfo)
	_, err = parameterModifier.AddValuesToExecuteFrame(f, prepareRequestInfo, variablesMetadata, replacementTimeUuids)
	require.NotNil(t, err)
}

func TestAddValuesToExecuteFrame_PositionalValues(t *testing.T) {
	now, err := uuid.NewUUID()
	require.Nil(t, err)
	type testParam struct {
		name         string
		replacedTerm *term
		paramType    datatype.DataType
		value        interface{}
	}

	type testCase struct {
		name                       string
		prepareContainsNamedValues bool
		testParams                 []*testParam
	}

	tests := []*testCase{
		{
			"three_parameters_two_generated",
			false,
			[]*testParam{
				{
					name:         "p0",
					// arity and start index are irrelevant here, they only matter when parsing/replacing the actual query string
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
				{
					name:         "p1",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval0",
				},
				{
					name:         "p2",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), 0),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
			},
		},
		{
			"two_parameters_one_generated",
			false,
			[]*testParam{
				{
					name:         "p0",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
				{
					name:         "p1",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval1",
				},
			},
		},
		{
			"two_parameters_one_generated_prepared_named_values",
			true,
			[]*testParam{
				{
					name:         "cloudgate__now",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
				{
					name:         "p1",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval1",
				},
			},
		},
		{
			"two_parameters_zero_generated",
			false,
			[]*testParam{
				{
					name:         "p0",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval1",
				},
				{
					name:         "p1",
					replacedTerm: nil,
					paramType:    datatype.Int,
					value:        3,
				},
			},
		},
		{
			"two_parameters_two_generated",
			false,
			[]*testParam{
				{
					name:         "p0",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
				{
					name:         "p1",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestPosVals := make([]*primitive.Value, 0)
			replacedTerms := make([]*term, 0)
			vm := &message.VariablesMetadata{
				PkIndices: nil,
				Columns:   []*message.ColumnMetadata{},
			}
			for idx, param := range test.testParams {
				if param.replacedTerm != nil {
					replacedTerms = append(replacedTerms, param.replacedTerm)
				} else {
					codec, err := datacodec.NewCodec(param.paramType)
					require.Nil(t, err)
					testVal, err := codec.Encode(param.value, primitive.ProtocolVersion4)
					requestPosVals = append(requestPosVals, primitive.NewValue(testVal))
				}
				vm.Columns = append(vm.Columns, &message.ColumnMetadata{
					Keyspace: "",
					Table:    "",
					Name:     param.name,
					Index:    int32(idx),
					Type:     param.paramType,
				})
			}

			generator, err := newTimeUuidGenerator()
			require.Nil(t, err)
			parameterModifier := NewParameterModifier(generator)
			queryOpts := &message.QueryOptions{PositionalValues: requestPosVals}
			clonedQueryOpts := queryOpts.Clone() // we use this so that we keep the "original" request options
			f := frame.NewFrame(primitive.ProtocolVersion4, 1, &message.Execute{
				QueryId:          nil,
				ResultMetadataId: nil,
				Options:          clonedQueryOpts,
			})
			containsPositionalMarkers := ((len(requestPosVals)+len(replacedTerms)) > 0) && !test.prepareContainsNamedValues
			prepareRequestInfo := NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), replacedTerms, containsPositionalMarkers, "", "")

			replacementTimeUuids := parameterModifier.generateTimeUuids(prepareRequestInfo)
			executeMsg, err := parameterModifier.AddValuesToExecuteFrame(f, prepareRequestInfo, vm, replacementTimeUuids)

			require.Nil(t, err)
			require.Equal(t, len(requestPosVals) + len(replacedTerms), len(executeMsg.Options.PositionalValues))
			var generatedValue *primitive.Value
			generatedCount := 0
			for idx, requestParamVal := range test.testParams {
				paramVal := executeMsg.Options.PositionalValues[idx]
				if requestParamVal.replacedTerm == nil {
					require.Equal(t, requestPosVals[idx-generatedCount], paramVal)
				} else {
					generatedCount++
					if generatedValue == nil {
						generatedValue = paramVal
					} else {
						require.NotEqual(t, generatedValue, paramVal)
						generatedValue = paramVal
					}
				}
			}
			if len(replacedTerms) == 0 {
				require.Nil(t, generatedValue)
			} else {
				require.NotNil(t, generatedValue)
				RequireValidGeneratedTime(t, now, generatedValue)
			}
		})
	}
}
func TestAddValuesToExecuteFrame_NamedValues(t *testing.T) {
	now, err := uuid.NewUUID()
	require.Nil(t, err)
	type testParam struct {
		name string
		replacedTerm *term
		paramType datatype.DataType
		value interface{}
	}

	type testCase struct {
		name       string
		testParams []*testParam
	}

	tests := []*testCase{
		{
			"three_parameters_two_generated",
			[]*testParam{
				{
					name:         "cloudgate__now",
					// arity and start index are irrelevant here, they only matter when parsing/replacing the actual query string
					// previousPositionalIndex is also irrelevant since we are using named values
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
				{
					name:         "param0",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval0",
				},
				{
					name:         "cloudgate__now",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
			},
		},
		{
			"two_parameters_one_generated",
			[]*testParam{
				{
					name:         "cloudgate__now",
					replacedTerm: NewFunctionCallTerm(NewFunctionCall("", "now", 0, 0, 0), -1),
					paramType:    datatype.Timeuuid,
					value:        nil,
				},
				{
					name:         "param1",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval1",
				},
			},
		},
		{
			"two_parameters_zero_generated",
			[]*testParam{
				{
					name:         "param1",
					replacedTerm: nil,
					paramType:    datatype.Ascii,
					value:        "testval1",
				},
				{
					name:         "param2",
					replacedTerm: nil,
					paramType:    datatype.Int,
					value:        3,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestNamedVals := map[string]*primitive.Value{}
			replacedTerms := make([]*term, 0)
			vm := &message.VariablesMetadata{
				PkIndices: nil,
				Columns:   []*message.ColumnMetadata{},
			}
			for idx, param := range test.testParams {
				if param.replacedTerm != nil {
					replacedTerms = append(replacedTerms, param.replacedTerm)
				} else {
					codec, err := datacodec.NewCodec(param.paramType)
					require.Nil(t, err)
					testVal, err := codec.Encode(param.value, primitive.ProtocolVersion4)
					requestNamedVals[param.name] = primitive.NewValue(testVal)
				}
				vm.Columns = append(vm.Columns, &message.ColumnMetadata{
					Keyspace: "",
					Table:    "",
					Name:     param.name,
					Index:    int32(idx),
					Type:     param.paramType,
				})
			}

			generator, err := newTimeUuidGenerator()
			require.Nil(t, err)
			parameterModifier := NewParameterModifier(generator)
			queryOpts := &message.QueryOptions{NamedValues: requestNamedVals}
			clonedQueryOpts := queryOpts.Clone() // we use this so that we keep the "original" request options
			f := frame.NewFrame(primitive.ProtocolVersion4, 1, &message.Execute{
				QueryId:          nil,
				ResultMetadataId: nil,
				Options:          clonedQueryOpts,
			})
			prepareRequestInfo := NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), replacedTerms, false, "", "")

			replacementTimeUuids := parameterModifier.generateTimeUuids(prepareRequestInfo)
			executeMsg, err := parameterModifier.AddValuesToExecuteFrame(f, prepareRequestInfo, vm, replacementTimeUuids)

			require.Nil(t, err)
			if len(replacedTerms) == 0 {
				require.Equal(t, len(requestNamedVals), len(executeMsg.Options.NamedValues))
			} else {
				require.Equal(t, len(requestNamedVals) + 1, len(executeMsg.Options.NamedValues))
			}
			for requestParamName, requestParamVal := range requestNamedVals {
				require.NotEqual(t, "cloudgate__now", requestParamName)
				paramVal, ok := executeMsg.Options.NamedValues[requestParamName]
				require.True(t, ok)
				require.Equal(t, requestParamVal, paramVal)
			}
			generatedValue, ok := executeMsg.Options.NamedValues["cloudgate__now"]
			if len(replacedTerms) == 0 {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				RequireValidGeneratedTime(t, now, generatedValue)
			}
		})
	}
}

func RequireValidGeneratedTime(t *testing.T, beforeTestTimeUuid uuid.UUID, generatedValue *primitive.Value) {
	var newTimeUuid primitive.UUID
	wasNull, err := datacodec.Timeuuid.Decode(generatedValue.Contents, &newTimeUuid, primitive.ProtocolVersion4)
	require.False(t, wasNull)
	require.Nil(t, err)
	newParsedTimeUuid, err := uuid.FromBytes(newTimeUuid.Bytes())
	require.Nil(t, err)
	require.GreaterOrEqual(t, int64(newParsedTimeUuid.Time()), int64(beforeTestTimeUuid.Time()))
	now, err := uuid.NewUUID()
	require.Nil(t, err)
	require.LessOrEqual(t, int64(newParsedTimeUuid.Time()), int64(now.Time()))
}