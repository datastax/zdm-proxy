package cloudgateproxy

import (
	"bytes"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	parser "github.com/riptano/cloud-gate/antlr"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync/atomic"
)

type forwardDecision string

const (
	forwardToOrigin = forwardDecision("origin")
	forwardToTarget = forwardDecision("target")
	forwardToBoth   = forwardDecision("both")
	forwardToNone   = forwardDecision("none")
)

type UnpreparedExecuteError struct {
	Header     *frame.Header
	Body       *frame.Body
	preparedId []byte
}

func (uee *UnpreparedExecuteError) Error() string {
	return fmt.Sprintf("The preparedID of the statement to be executed (%s) does not exist in the proxy cache", uee.preparedId)
}

func inspectFrame(
	f *frame.RawFrame,
	psCache *PreparedStatementCache,
	mh metrics.IMetricsHandler,
	currentKeyspaceName *atomic.Value,
	forwardReadsToTarget bool) (StatementInfo, error) {

	forwardDecision := forwardToBoth

	switch f.Header.OpCode {

	case primitive.OpCodeQuery:
		body, err := defaultCodec.DecodeBody(f.Header, bytes.NewReader(f.Body))
		if err != nil {
			return nil, fmt.Errorf("could not decode body of query message: %w", err)
		}
		queryMsg, ok := body.Message.(*message.Query)
		if !ok {
			return nil, fmt.Errorf("expected Query but got %v instead", body.Message.GetOpCode())
		}
		queryInfo := inspectCqlQuery(queryMsg.Query)
		if queryInfo.getStatementType() == statementTypeSelect {
			if isSystemQuery(queryInfo, currentKeyspaceName) || forwardReadsToTarget {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
		return NewGenericStatementInfo(forwardDecision), nil

	case primitive.OpCodePrepare:
		body, err := defaultCodec.DecodeBody(f.Header, bytes.NewReader(f.Body))
		if err != nil {
			return nil, fmt.Errorf("could not decode body of prepare message: %w", err)
		}
		prepareMsg, ok := body.Message.(*message.Prepare)
		if !ok {
			return nil, fmt.Errorf("expected Prepare but got %v instead", body.Message.GetOpCode())
		}
		queryInfo := inspectCqlQuery(prepareMsg.Query)
		if queryInfo.getStatementType() == statementTypeSelect {
			if isSystemQuery(queryInfo, currentKeyspaceName) || forwardReadsToTarget {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
		return NewPreparedStatementInfo(forwardDecision), nil

	case primitive.OpCodeExecute:
		body, err := defaultCodec.DecodeBody(f.Header, bytes.NewReader(f.Body))
		if err != nil {
			return nil, fmt.Errorf("could not decode body of execute message: %w", err)
		}
		executeMsg, ok := body.Message.(*message.Execute)
		if !ok {
			return nil, fmt.Errorf("expected Execute but got %v instead", body.Message.GetOpCode())
		}
		log.Debugf("Execute with prepared-id = '%s'", executeMsg.QueryId)
		if stmtInfo, ok := psCache.retrieveStmtInfoFromCache(executeMsg.QueryId); ok {
			// The forward decision was set in the cache when handling the corresponding PREPARE request
			return NewGenericStatementInfo(stmtInfo.forwardDecision), nil
		} else {
			log.Warnf("No cached entry for prepared-id = '%s'", executeMsg.QueryId)
			_ = mh.IncrementCountByOne(metrics.PSCacheMissCount)
			// return meaningful error to caller so it can generate an unprepared response
			return nil, &UnpreparedExecuteError{Header: f.Header, Body: body, preparedId: executeMsg.QueryId}
		}

	case primitive.OpCodeStartup, primitive.OpCodeAuthResponse:
		return NewGenericStatementInfo(forwardToOrigin), nil

	default:
		return NewGenericStatementInfo(forwardToBoth), nil
	}
}

func isSystemQuery(info queryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}
	return keyspaceName == "system" ||
		strings.HasPrefix(keyspaceName, "system_") ||
		strings.HasPrefix(keyspaceName, "dse_")
}

type statementType string

const (
	statementTypeSelect = statementType("select")
	statementTypeOther  = statementType("other")
)

type queryInfo interface {
	getStatementType() statementType
	getKeyspaceName() string
	getTableName() string
}

func inspectCqlQuery(query string) queryInfo {
	is := antlr.NewInputStream(query)
	lexer := parser.NewSimplifiedCqlLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	cqlParser := parser.NewSimplifiedCqlParser(stream)
	listener := &cqlListener{statementType: statementTypeOther}
	antlr.ParseTreeWalkerDefault.Walk(listener, cqlParser.CqlStatement())
	return listener
}

type cqlListener struct {
	*parser.BaseSimplifiedCqlListener
	statementType statementType
	keyspaceName  string
	tableName     string
}

//goland:noinspection GoUnusedParameter
func (l *cqlListener) ExitSelectStatement(ctx *parser.SelectStatementContext) {
	l.statementType = statementTypeSelect
}

func (l *cqlListener) ExitKeyspaceName(ctx *parser.KeyspaceNameContext) {
	identifier := ctx.Identifier().(*parser.IdentifierContext)
	l.keyspaceName = extractIdentifier(identifier)
}

func (l *cqlListener) ExitTableName(ctx *parser.TableNameContext) {
	for _, child := range ctx.QualifiedIdentifier().GetChildren() {
		if identifierContext, ok := child.(*parser.IdentifierContext); ok {
			l.tableName = extractIdentifier(identifierContext)
		}
	}
}

// Returns the identifier in the context object, in its internal form.
// For unquoted identifiers, the internal form is the form in full lower case;
// for quoted ones, the internal form is the unquoted string, in its exact case.
func extractIdentifier(identifierContext *parser.IdentifierContext) string {
	unquotedIdentifier := identifierContext.UNQUOTED_IDENTIFIER()
	if unquotedIdentifier != nil {
		return strings.ToLower(unquotedIdentifier.GetText())
	} else {
		quotedIdentifier := identifierContext.QUOTED_IDENTIFIER().GetText()
		// remove surrounding quotes
		quotedIdentifier = quotedIdentifier[1 : len(quotedIdentifier)-1]
		// handle escaped double-quotes
		quotedIdentifier = strings.ReplaceAll(quotedIdentifier, "\"\"", "\"")
		return quotedIdentifier
	}
}

func (l *cqlListener) getStatementType() statementType {
	return l.statementType
}

func (l *cqlListener) getKeyspaceName() string {
	return l.keyspaceName
}

func (l *cqlListener) getTableName() string {
	return l.tableName
}
