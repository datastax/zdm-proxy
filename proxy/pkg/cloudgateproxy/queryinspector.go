package cloudgateproxy

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/google/uuid"
	parser "github.com/riptano/cloud-gate/antlr"
	"strings"
)

type statementType string

const (
	statementTypeInsert = statementType("insert")
	statementTypeUpdate = statementType("update")
	statementTypeDelete = statementType("delete")
	statementTypeBatch  = statementType("batch")
	statementTypeSelect = statementType("select")
	statementTypeUse    = statementType("use")
	statementTypeOther  = statementType("other")
)

type queryInfo interface {
	getQuery() string
	getStatementType() statementType
	getKeyspaceName() string
	getTableName() string

	// Below methods are only relevant for INSERT statements,
	// or BATCH statements containing INSERT statements.

	// Returns a slice of assignmentsGroup for each statement in the query.
	// For a single INSERT, the slice contains only one element. For BATCH statements,
	// the slice will contain as many elements as there are child statements.
	getAssignmentsGroups() []*assignmentsGroup

	// Whether the query contains positional bind markers. Only one of hasPositionalBindMarkers and hasNamedBindMarkers
	// can return true for a given query, never both.
	// This will always be false for non-INSERT statements or batches not containing INSERT statements.
	hasPositionalBindMarkers() bool

	// Whether the query contains named bind markers. Only one of hasPositionalBindMarkers and hasNamedBindMarkers
	// can return true for a given query, never both.
	// This will always be false for non-INSERT statements or batches not containing INSERT statements.
	hasNamedBindMarkers() bool

	// Whether the query contains at least one now() function call.
	// This will always be false for non-INSERT statements or batches not containing INSERT statements.
	hasNowFunctionCalls() bool

	replaceNowFunctionCallsWithLiteral(replacement uuid.UUID) queryInfo
	replaceNowFunctionCallsWithPositionalBindMarkers() queryInfo
	replaceNowFunctionCallsWithNamedBindMarkers() queryInfo
}

func inspectCqlQuery(query string) queryInfo {
	is := antlr.NewInputStream(query)
	lexer := parser.NewSimplifiedCqlLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	cqlParser := parser.NewSimplifiedCqlParser(stream)
	listener := &cqlListener{query: query, statementType: statementTypeOther}
	antlr.ParseTreeWalkerDefault.Walk(listener, cqlParser.CqlStatement())
	return listener
}

type functionCall struct {
	keyspace   string
	name       string
	arity      int
	startIndex int
	stopIndex  int
}

func (f *functionCall) isNow() bool {
	return (f.keyspace == "" || f.keyspace == "system") && f.name == "now" && f.arity == 0
}

// A group of assignments belonging to the same statement.
type assignmentsGroup struct {
	// The zero-based index of the statement. For single INSERT statements, this will be zero. For BATCH child
	// statements, this will be the child index.
	statementIndex int
	statementType  statementType
	assignments    []*assignment
}

// An assignment in a CQL INSERT query referencing the column being updated, and the updated value.
// The updated value can be either a literal term, a function call, or a bind marker.
type assignment struct {

	// The name of the column being assigned; never empty.
	columnName string

	// The function call details in this assignment,
	// or nil if this assignment does not contain a function call.
	functionCall *functionCall

	// The zero-based index of the positional bind marker in this assignment
	// (-1 if this assignment does not contain a positional bind marker).
	positionalIndex int

	// The variable name of the named bind marker in this assignment
	// (empty if this assignment does not contain a named bind marker).
	bindMarkerName string

	// The literal expression in this assignment, or empty if this assignment does not contain a literal.
	literal string
}

func (a *assignment) isFunctionCall() bool {
	return a.functionCall != nil
}

func (a *assignment) isPositionalBindMarker() bool {
	return a.positionalIndex != -1
}

func (a *assignment) isNamedBindMarker() bool {
	return a.bindMarkerName != ""
}

func (a *assignment) isLiteral() bool {
	return a.literal != ""
}

type cqlListener struct {
	*parser.BaseSimplifiedCqlListener
	query         string
	statementType statementType
	keyspaceName  string
	tableName     string

	// Only filled in for INSERT and BATCH statements
	assignmentsGroups     []*assignmentsGroup
	positionalBindMarkers bool
	namedBindMarkers      bool
	nowFunctionCalls      bool

	// internal counters
	currentPositionalIndex int
	currentBatchChildIndex int
}

func (l *cqlListener) getQuery() string {
	return l.query
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

func (l *cqlListener) getAssignmentsGroups() []*assignmentsGroup {
	return l.assignmentsGroups
}

func (l *cqlListener) hasPositionalBindMarkers() bool {
	return l.positionalBindMarkers
}

func (l *cqlListener) hasNamedBindMarkers() bool {
	return l.namedBindMarkers
}

func (l *cqlListener) hasNowFunctionCalls() bool {
	return l.nowFunctionCalls
}

func (l *cqlListener) EnterCqlStatement(ctx *parser.CqlStatementContext) {
	if ctx.InsertStatement() != nil {
		l.statementType = statementTypeInsert
	} else if ctx.UpdateStatement() != nil {
		l.statementType = statementTypeUpdate
	} else if ctx.DeleteStatement() != nil {
		l.statementType = statementTypeDelete
	} else if ctx.BatchStatement() != nil {
		l.statementType = statementTypeBatch
	} else if ctx.SelectStatement() != nil {
		l.statementType = statementTypeSelect
	} else if ctx.UseStatement() != nil {
		l.statementType = statementTypeUse
	}
}

func (l *cqlListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	assignmentsGroup := &assignmentsGroup{statementIndex: l.currentBatchChildIndex, statementType: statementTypeInsert}
	columns := ctx.Identifiers().(*parser.IdentifiersContext)
	values := ctx.Terms().(*parser.TermsContext)
	// if the lengths do not match, the statement will be rejected by the server
	if len(columns.AllIdentifier()) == len(values.AllTerm()) {
		for i, identifierCtx := range columns.AllIdentifier() {
			termCtx := values.AllTerm()[i]
			assignment := l.extractAssignment(identifierCtx.(*parser.IdentifierContext), termCtx.(*parser.TermContext))
			assignmentsGroup.assignments = append(assignmentsGroup.assignments, assignment)
		}
	}
	l.assignmentsGroups = append(l.assignmentsGroups, assignmentsGroup)
	l.currentBatchChildIndex++
}

func (l *cqlListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	if _, batchChild := ctx.GetParent().(*parser.BatchChildStatementContext); batchChild {
		assignmentsGroup := &assignmentsGroup{statementIndex: l.currentBatchChildIndex, statementType: statementTypeUpdate}
		l.assignmentsGroups = append(l.assignmentsGroups, assignmentsGroup)
		l.currentBatchChildIndex++
	}
}

func (l *cqlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	if _, batchChild := ctx.GetParent().(*parser.BatchChildStatementContext); batchChild {
		assignmentsGroup := &assignmentsGroup{statementIndex: l.currentBatchChildIndex, statementType: statementTypeDelete}
		l.assignmentsGroups = append(l.assignmentsGroups, assignmentsGroup)
		l.currentBatchChildIndex++
	}
}

func (l *cqlListener) EnterUseStatement(ctx *parser.UseStatementContext) {
	l.keyspaceName = extractIdentifier(ctx.KeyspaceName().(*parser.KeyspaceNameContext).Identifier().(*parser.IdentifierContext))
}

func (l *cqlListener) EnterTableName(ctx *parser.TableNameContext) {
	// Note: this will capture the *last* table name in a BATCH statement
	if ctx.QualifiedIdentifier().GetChildCount() == 1 {
		identifierContext := ctx.QualifiedIdentifier().GetChild(0).(*parser.IdentifierContext)
		l.tableName = extractIdentifier(identifierContext)
	} else {
		// 3 children: keyspaceName, token DOT, identifier
		keyspaceNameContext := ctx.QualifiedIdentifier().GetChild(0).(*parser.KeyspaceNameContext)
		l.keyspaceName = extractIdentifier(keyspaceNameContext.Identifier().(*parser.IdentifierContext))
		identifierContext := ctx.QualifiedIdentifier().GetChild(2).(*parser.IdentifierContext)
		l.tableName = extractIdentifier(identifierContext)
	}
}

func (l *cqlListener) extractAssignment(identifierCtx *parser.IdentifierContext, termCtx *parser.TermContext) *assignment {
	var a *assignment
	if typeCastCtx := termCtx.TypeCast(); typeCastCtx != nil {
		// extract the term being cast and ignore the target type of the cast expression
		a = l.extractAssignment(identifierCtx, typeCastCtx.(*parser.TypeCastContext).Term().(*parser.TermContext))
	} else {
		a = &assignment{columnName: extractIdentifier(identifierCtx), positionalIndex: -1}
		if literalCtx := termCtx.Literal(); literalCtx != nil {
			a.literal = literalCtx.GetText()
		} else if functionCallCtx := termCtx.FunctionCall(); functionCallCtx != nil {
			a.functionCall = extractFunctionCall(functionCallCtx.(*parser.FunctionCallContext))
			if a.functionCall.isNow() {
				l.nowFunctionCalls = true
			}
		} else if bindMarkerCtx := termCtx.BindMarker(); bindMarkerCtx != nil {
			positionalBindMarkerCtx := bindMarkerCtx.(*parser.BindMarkerContext).PositionalBindMarker()
			if positionalBindMarkerCtx != nil {
				l.positionalBindMarkers = true
				a.positionalIndex = l.currentPositionalIndex
				l.currentPositionalIndex++
			} else {
				namedBindMarkerCtx := bindMarkerCtx.(*parser.BindMarkerContext).NamedBindMarker()
				if namedBindMarkerCtx != nil {
					l.namedBindMarkers = true
					bindMarkerName := extractIdentifier(
						namedBindMarkerCtx.(*parser.NamedBindMarkerContext).
							Identifier().(*parser.IdentifierContext))
					a.bindMarkerName = bindMarkerName
				}
			}
		}
	}
	return a
}

func extractFunctionCall(ctx *parser.FunctionCallContext) *functionCall {
	qualifiedIdentifierContext := ctx.
		FunctionName().(*parser.FunctionNameContext).
		QualifiedIdentifier().(*parser.QualifiedIdentifierContext)
	keyspaceName := ""
	if qualifiedIdentifierContext.KeyspaceName() != nil {
		keyspaceName = extractIdentifier(
			qualifiedIdentifierContext.KeyspaceName().(*parser.KeyspaceNameContext).
				Identifier().(*parser.IdentifierContext))
	}
	functionName := extractIdentifier(qualifiedIdentifierContext.Identifier().(*parser.IdentifierContext))
	// For now we only record the function arity, not the actual function arguments
	functionArity := 0
	if ctx.FunctionArgs() != nil {
		functionArity = len(ctx.FunctionArgs().(*parser.FunctionArgsContext).AllFunctionArg())
	}
	start := ctx.GetStart().GetStart()
	stop := ctx.GetStop().GetStop()
	return &functionCall{
		keyspace:   keyspaceName,
		name:       functionName,
		arity:      functionArity,
		startIndex: start,
		stopIndex:  stop,
	}
}

// Returns the identifier in the context object, in its internal form.
// For unquoted identifiers and unreserved keywords, the internal form is the form in full lower case;
// for quoted ones, the internal form is the unquoted string, in its exact case.
func extractIdentifier(identifierContext *parser.IdentifierContext) string {
	if unquotedIdentifier := identifierContext.UNQUOTED_IDENTIFIER(); unquotedIdentifier != nil {
		return strings.ToLower(unquotedIdentifier.GetText())
	} else if quotedIdentifier := identifierContext.QUOTED_IDENTIFIER(); quotedIdentifier != nil {
		identifier := quotedIdentifier.GetText()
		// remove surrounding quotes
		identifier = identifier[1 : len(identifier)-1]
		// handle escaped double-quotes
		identifier = strings.ReplaceAll(identifier, "\"\"", "\"")
		return identifier
	} else {
		return strings.ToLower(identifierContext.UnreservedKeyword().GetText())
	}
}

func (l *cqlListener) replaceFunctionCalls(replacementFunc func(query string, functionCall *functionCall) *string) queryInfo {
	if !l.hasNowFunctionCalls() {
		return l
	}
	var result string
	i := 0
	for _, assignments := range l.assignmentsGroups {
		for _, assignment := range assignments.assignments {
			if assignment.isFunctionCall() {
				replacement := replacementFunc(l.query, assignment.functionCall)
				if replacement != nil {
					result = result + l.query[i:assignment.functionCall.startIndex] + *replacement
					i = assignment.functionCall.stopIndex + 1
				}
			}
		}
	}
	result = result + l.query[i:len(l.query)]
	return inspectCqlQuery(result)
}

func (l *cqlListener) replaceNowFunctionCallsWithLiteral(literal uuid.UUID) queryInfo {
	replacement := literal.String()
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) *string {
		if functionCall.isNow() {
			return &replacement
		} else {
			return nil
		}
	})
}

func (l *cqlListener) replaceNowFunctionCallsWithPositionalBindMarkers() queryInfo {
	var questionMark = "?"
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) *string {
		if functionCall.isNow() {
			return &questionMark
		} else {
			return nil
		}
	})
}

func (l *cqlListener) replaceNowFunctionCallsWithNamedBindMarkers() queryInfo {
	counter := 0
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) *string {
		if functionCall.isNow() {
			name := fmt.Sprint(":cloudgate__now__", counter)
			counter++
			return &name
		} else {
			return nil
		}
	})
}
