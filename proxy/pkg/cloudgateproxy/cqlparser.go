package cloudgateproxy

import (
	"github.com/antlr/antlr4/runtime/Go/antlr"
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

func inspectFrame(f *Frame, psCache *PreparedStatementCache, mh metrics.IMetricsHandler, currentKeyspaceName *atomic.Value) (forwardDecision, error) {

	forwardDecision := forwardToBoth

	switch f.Opcode {

	case OpCodeQuery:
		query, err := readLongString(f.Body)
		if err != nil {
			return forwardToNone, err
		}
		queryInfo := inspectCqlQuery(query)
		if queryInfo.getStatementType() == statementTypeSelect {
			if isSystemLocalOrSystemPeers(queryInfo, currentKeyspaceName) {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
		return forwardDecision, nil

	case OpCodePrepare:
		query, err := readLongString(f.Body)
		if err != nil {
			return forwardToNone, err
		}
		queryInfo := inspectCqlQuery(query)
		if queryInfo.getStatementType() == statementTypeSelect {
			if isSystemLocalOrSystemPeers(queryInfo, currentKeyspaceName) {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
		psCache.trackStatementToBePrepared(f.StreamId, forwardDecision)
		return forwardDecision, nil

	case OpCodeExecute:
		preparedId, err := readShortBytes(f.Body)
		if err != nil {
			return forwardToNone, err
		}
		log.Debugf("Execute with prepared-id = '%s'", preparedId)
		if stmtInfo, ok := psCache.retrieveStmtInfoFromCache(preparedId); ok {
			// The forward decision was set in the cache when handling the corresponding PREPARE request
			return stmtInfo.forwardDecision, nil
		} else {
			log.Warnf("No cached entry for prepared-id = '%s'", preparedId)
			_ = mh.IncrementCountByOne(metrics.PSCacheMissCount)
			// TODO handle cache miss here! Generate an UNPREPARED response and send straight back to the client?
			return forwardToBoth, nil
		}

	case OpCodeRegister, OpCodeStartup, OpCodeAuthResponseRequest:
		return forwardToOrigin, nil

	default:
		return forwardToBoth, nil
	}
}

func isSystemLocalOrSystemPeers(info queryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}
	if keyspaceName == "system" {
		tableName := info.getTableName()
		if tableName == "local" ||
			tableName == "peers" ||
			tableName == "peers_v2" {
			return true
		}
	}
	return false
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
