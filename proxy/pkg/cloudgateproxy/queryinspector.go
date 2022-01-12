package cloudgateproxy

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/google/uuid"
	parser "github.com/riptano/cloud-gate/antlr"
	log "github.com/sirupsen/logrus"
	"sort"
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

	cloudgateNowNamedMarker = "cloudgate__now"
)

var (
	sortedCloudgateNamedMarkers = []string{cloudgateNowNamedMarker}
)

func init() {
	sort.Strings(sortedCloudgateNamedMarkers)
}

type QueryInfo interface {
	getQuery() string
	getStatementType() statementType
	getKeyspaceName() string
	getTableName() string

	// Below methods are only relevant for INSERT statements,
	// or BATCH statements containing INSERT statements.

	// Returns a slice of parsedStatement. There is one parsedStatement per statement in the query.
	// For a single INSERT/UPDATE/DELETE, the slice contains only one element. For BATCH statements,
	// the slice will contain as many elements as there are child statements.
	getParsedStatements() []*parsedStatement

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

	replaceNowFunctionCallsWithLiteral(replacement uuid.UUID) (QueryInfo, []*term)
	replaceNowFunctionCallsWithPositionalBindMarkers() (QueryInfo, []*term)
	replaceNowFunctionCallsWithNamedBindMarkers() (QueryInfo, []*term)
}

func inspectCqlQuery(query string) QueryInfo {
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

func NewFunctionCall(keyspace string, name string, arity int, startIndex int, stopIndex int) *functionCall {
	return &functionCall{
		keyspace:   keyspace,
		name:       name,
		arity:      arity,
		startIndex: startIndex,
		stopIndex:  stopIndex,
	}
}

func (f *functionCall) isNow() bool {
	return (f.keyspace == "" || f.keyspace == "system") && f.name == "now" && f.arity == 0
}

// parsedStatement contains all the information stored by the cqlListener while processing a particular statement.
type parsedStatement struct {
	// The zero-based index of the statement. For single INSERT/UPDATE/DELETE statements, this will be zero. For BATCH child
	// statements, this will be the child index.
	statementIndex int
	statementType  statementType
	terms          []*term
}

// A term can be one of the following:
// - a literal,
// - a function call
// - a bind marker.
type term struct {
	// The function call details in this term,
	// or nil if this term does not contain a function call.
	functionCall *functionCall

	// The index of the closest (to the left) positional bind marker.
	// (-1 if no positional bind marker exists to the left of this term).
	previousPositionalIndex int

	// The zero-based index of the positional bind marker in this term
	// (-1 if this term does not contain a positional bind marker).
	positionalIndex int

	// The variable name of the named bind marker in this term
	// (empty if this term does not contain a named bind marker).
	bindMarkerName string

	// The literal expression in this term, or empty if this term does not contain a literal.
	literal string
}

func NewNamedBindMarkerTerm(name string, previousPositionalIndex int) *term {
	return &term{
		functionCall:            nil,
		positionalIndex:         -1,
		previousPositionalIndex: previousPositionalIndex,
		bindMarkerName:          name,
		literal:                 "",
	}
}

func NewPositionalBindMarkerTerm(position int) *term {
	return &term{
		functionCall:            nil,
		positionalIndex:         position,
		previousPositionalIndex: position - 1,
		bindMarkerName:          "",
		literal:                 "",
	}
}

func NewLiteralTerm(literal string, previousPositionalIndex int) *term {
	return &term{
		functionCall:            nil,
		positionalIndex:         -1,
		previousPositionalIndex: previousPositionalIndex,
		bindMarkerName:          "",
		literal:                 literal,
	}
}

func NewFunctionCallTerm(functionCall *functionCall, previousPositionalIndex int) *term {
	return &term{
		functionCall:            functionCall,
		positionalIndex:         -1,
		previousPositionalIndex: previousPositionalIndex,
		bindMarkerName:          "",
		literal:                 "",
	}
}

func (t *term) isFunctionCall() bool {
	return t.functionCall != nil
}

func (t *term) isPositionalBindMarker() bool {
	return t.positionalIndex != -1
}

func (t *term) isNamedBindMarker() bool {
	return t.bindMarkerName != ""
}

func (t *term) isLiteral() bool {
	return t.literal != ""
}

type cqlListener struct {
	*parser.BaseSimplifiedCqlListener
	query         string
	statementType statementType
	keyspaceName  string
	tableName     string

	// Only filled in for INSERT, DELETE, UPDATE and BATCH statements
	parsedStatements      []*parsedStatement
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

func (l *cqlListener) getParsedStatements() []*parsedStatement {
	return l.parsedStatements
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
	parsedStatement := &parsedStatement{statementIndex: l.currentBatchChildIndex, statementType: statementTypeInsert}
	values := ctx.Terms().(*parser.TermsContext)
	for _, termCtx := range values.AllTerm() {
		parsedStatement.terms = append(parsedStatement.terms, l.extractTerm(termCtx.(*parser.TermContext)))
	}

	usingClauseCtx := ctx.UsingClause()
	if usingClauseCtx != nil {
		parsedStatement.terms = append(parsedStatement.terms, l.extractUsingClauseBindMarkers(usingClauseCtx.(*parser.UsingClauseContext))...)
	}

	l.parsedStatements = append(l.parsedStatements, parsedStatement)
	l.currentBatchChildIndex++
}

func (l *cqlListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	parsedStatement := &parsedStatement{statementIndex: l.currentBatchChildIndex, statementType: statementTypeUpdate}

	usingClauseCtx := ctx.UsingClause()
	if usingClauseCtx != nil {
		parsedStatement.terms = append(parsedStatement.terms, l.extractUsingClauseBindMarkers(usingClauseCtx.(*parser.UsingClauseContext))...)
	}

	updateOperations := ctx.UpdateOperations().(*parser.UpdateOperationsContext).AllUpdateOperation()
	for _, updateOperation := range updateOperations {
		updateOperationTyped := updateOperation.(*parser.UpdateOperationContext)
		for _, termCtx := range updateOperationTyped.AllTerm() {
			parsedStatement.terms = append(parsedStatement.terms, l.extractTerm(termCtx.(*parser.TermContext)))
		}
	}

	whereClauseTerms := l.extractWhereClauseTerms(ctx.WhereClause().(*parser.WhereClauseContext))
	parsedStatement.terms = append(parsedStatement.terms, whereClauseTerms...)

	if conditionsCtx := ctx.Conditions(); conditionsCtx != nil {
		conditionTerms := l.extractConditionsTerms(conditionsCtx.(*parser.ConditionsContext))
		parsedStatement.terms = append(parsedStatement.terms, conditionTerms...)
	}

	l.parsedStatements = append(l.parsedStatements, parsedStatement)
	l.currentBatchChildIndex++
}

func (l *cqlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	parsedStmt := &parsedStatement{statementIndex: l.currentBatchChildIndex, statementType: statementTypeDelete}

	if deleteOperationsCtx := ctx.DeleteOperations(); deleteOperationsCtx != nil {
		deleteOperations := deleteOperationsCtx.(*parser.DeleteOperationsContext).AllDeleteOperation()
		for _, deleteOperation := range deleteOperations {
			deleteOperationTyped := deleteOperation.(*parser.DeleteOperationContext)
			t := deleteOperationTyped.Term()
			if t != nil {
				parsedStmt.terms = append(parsedStmt.terms, l.extractTerm(t.(*parser.TermContext)))
			}
		}
	}

	if timestampCtx := ctx.Timestamp(); timestampCtx != nil {
		parsedTimestampCtx := timestampCtx.(*parser.TimestampContext)
		timeStampTerm := l.extractNillableBindMarker(parsedTimestampCtx.BindMarker())
		if timeStampTerm != nil {
			parsedStmt.terms = append(parsedStmt.terms, timeStampTerm)
		}
	}

	whereClauseTerms := l.extractWhereClauseTerms(ctx.WhereClause().(*parser.WhereClauseContext))
	parsedStmt.terms = append(parsedStmt.terms, whereClauseTerms...)

	if conditionsCtx := ctx.Conditions(); conditionsCtx != nil {
		conditionTerms := l.extractConditionsTerms(conditionsCtx.(*parser.ConditionsContext))
		parsedStmt.terms = append(parsedStmt.terms, conditionTerms...)
	}

	l.parsedStatements = append(l.parsedStatements, parsedStmt)
	l.currentBatchChildIndex++
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

func (l *cqlListener) extractAllTerms(termsCtx []parser.ITermContext) []*term {
	var terms []*term
	for _, termCtx := range termsCtx {
		terms = append(terms, l.extractTerm(termCtx.(*parser.TermContext)))
	}
	return terms
}

func (l *cqlListener) extractTerms(termsCtx *parser.TermsContext) []*term {
	return l.extractAllTerms(termsCtx.AllTerm())
}

func (l *cqlListener) extractTerm(termCtx *parser.TermContext) *term {
	if typeCastCtx := termCtx.TypeCast(); typeCastCtx != nil {
		// extract the term being cast and ignore the target type of the cast expression
		return l.extractTerm(typeCastCtx.(*parser.TypeCastContext).Term().(*parser.TermContext))
	} else {
		if literalCtx := termCtx.Literal(); literalCtx != nil {
			return NewLiteralTerm(literalCtx.GetText(), l.currentPositionalIndex - 1)
		} else if functionCallCtx := termCtx.FunctionCall(); functionCallCtx != nil {
			functionCall := extractFunctionCall(functionCallCtx.(*parser.FunctionCallContext))
			if functionCall.isNow() {
				l.nowFunctionCalls = true
			}
			return NewFunctionCallTerm(functionCall, l.currentPositionalIndex - 1)
		} else if bindMarkerCtx := termCtx.BindMarker(); bindMarkerCtx != nil {
			return l.extractBindMarker(bindMarkerCtx.(*parser.BindMarkerContext))
		} else {
			return nil
		}
	}
}

func (l *cqlListener) extractAllBindMarkers(allBindMarkersCtx []parser.IBindMarkerContext) []*term {
	var terms []*term
	for _, bindMarkerCtx := range allBindMarkersCtx {
		terms = append(terms, l.extractBindMarker(bindMarkerCtx.(*parser.BindMarkerContext)))
	}
	return terms
}

func (l *cqlListener) extractBindMarkers(bindMarkersCtx *parser.BindMarkersContext) []*term {
	return l.extractAllBindMarkers(bindMarkersCtx.AllBindMarker())
}

func (l *cqlListener) extractBindMarker(bindMarkerCtx *parser.BindMarkerContext) *term {
	if positionalBindMarkerCtx := bindMarkerCtx.PositionalBindMarker(); positionalBindMarkerCtx != nil {
		l.positionalBindMarkers = true
		newTerm := NewPositionalBindMarkerTerm(l.currentPositionalIndex)
		l.currentPositionalIndex++
		return newTerm
	} else if namedBindMarkerCtx := bindMarkerCtx.NamedBindMarker(); namedBindMarkerCtx != nil {
		l.namedBindMarkers = true
		bindMarkerName := extractIdentifier(
			namedBindMarkerCtx.(*parser.NamedBindMarkerContext).
				Identifier().(*parser.IdentifierContext))
		return NewNamedBindMarkerTerm(bindMarkerName, l.currentPositionalIndex - 1)
	} else {
		log.Errorf("Could not parse bind marker: %v", bindMarkerCtx.GetText())
		return nil
	}
}

func (l *cqlListener) extractWhereClauseTerms(ctx *parser.WhereClauseContext) []*term {
	var terms []*term

	relations := ctx.AllRelation()
	for _, relation := range relations {
		relationTyped := relation.(*parser.RelationContext)
		terms = append(terms, l.extractRelationTerms(relationTyped)...)
	}

	return terms
}

func (l *cqlListener) extractRelationTerms(ctx *parser.RelationContext) []*term {
	if relationCtx := ctx.Relation(); relationCtx != nil {
		return l.extractRelationTerms(relationCtx.(*parser.RelationContext))
	}

	// looking at the grammar, relation can only have one of: term, terms, bindmarker, bindmarkers, tupleliteral or tupleliterals
	// so we don't have to worry about order of the markers as there is no risk of mixing markers from different context types
	if termsCtx := ctx.AllTerm(); len(termsCtx) > 0 {
		return l.extractAllTerms(termsCtx)
	}

	if termsCtx := ctx.Terms(); termsCtx != nil {
		return l.extractTerms(termsCtx.(*parser.TermsContext))
	}

	if bindMarkersCtx := ctx.BindMarkers(); bindMarkersCtx != nil {
		return l.extractBindMarkers(bindMarkersCtx.(*parser.BindMarkersContext))
	}

	if bindMarkerCtx := ctx.BindMarker(); bindMarkerCtx != nil {
		return []*term{l.extractBindMarker(bindMarkerCtx.(*parser.BindMarkerContext))}
	}

	if tupleLiteralCtx := ctx.TupleLiteral(); tupleLiteralCtx != nil {
		if termsCtx := tupleLiteralCtx.(*parser.TupleLiteralContext).Terms(); termsCtx != nil {
			return l.extractTerms(termsCtx.(*parser.TermsContext))
		}

		return []*term{}
	}

	if tupleLiteralsCtx := ctx.TupleLiterals(); tupleLiteralsCtx != nil {
		allTupleLiteralsCtx := tupleLiteralsCtx.(*parser.TupleLiteralsContext).AllTupleLiteral()
		newTerms := make([]*term, 0)
		for _, tupleLiteralCtx := range allTupleLiteralsCtx {
			if termsCtx := tupleLiteralCtx.(*parser.TupleLiteralContext).Terms(); termsCtx != nil {
				newTerms = append(newTerms, l.extractTerms(termsCtx.(*parser.TermsContext))...)
			}
		}

		return newTerms
	}

	return []*term{}
}

func (l *cqlListener) extractConditionsTerms(ctx *parser.ConditionsContext) []*term {
	var terms []*term

	conditions := ctx.AllCondition()
	for _, condition := range conditions {
		conditionTyped := condition.(*parser.ConditionContext)

		// looking at the grammar, condition can have term + one of either terms or bindmarker, so we must ensure to
		// parse term first
		if allTermCtx := conditionTyped.AllTerm(); allTermCtx != nil {
			terms = append(terms, l.extractAllTerms(allTermCtx)...)
		}

		if termsCtx := conditionTyped.Terms(); termsCtx != nil {
			terms = append(terms, l.extractTerms(termsCtx.(*parser.TermsContext))...)
		} else if bindMarkerCtx := conditionTyped.BindMarker(); bindMarkerCtx != nil {
			terms = append(terms, l.extractBindMarker(bindMarkerCtx.(*parser.BindMarkerContext)))
		}
	}

	return terms
}

func (l *cqlListener) extractUsingClauseBindMarkers(ctx *parser.UsingClauseContext) []*term {
	var terms []*term

	for _, childCtx := range ctx.GetChildren() {
		var bindMarkerTerm *term
		switch parsedCtx := childCtx.(type) {
		case parser.ITimestampContext:
			bindMarkerTerm = l.extractNillableBindMarker(parsedCtx.(*parser.TimestampContext).BindMarker())
		case parser.ITtlContext:
			bindMarkerTerm = l.extractNillableBindMarker(parsedCtx.(*parser.TtlContext).BindMarker())
		}

		if bindMarkerTerm != nil {
			terms = append(terms, bindMarkerTerm)
		}
	}

	return terms
}

func (l *cqlListener) extractNillableBindMarker(ctx parser.IBindMarkerContext) *term {
	if ctx != nil {
		return l.extractBindMarker(ctx.(*parser.BindMarkerContext))
	}

	return nil
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
	return NewFunctionCall(
		keyspaceName,
		functionName,
		functionArity,
		start,
		stop)
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

func (l *cqlListener) replaceFunctionCalls(replacementFunc func(query string, functionCall *functionCall) *string) (QueryInfo, []*term) {
	if !l.hasNowFunctionCalls() {
		return l, make([]*term, 0)
	}
	var result string
	i := 0
	replacedTerms := make([]*term, 0)
	for _, parsedStatement := range l.parsedStatements {
		for _, term := range parsedStatement.terms {
			if term.isFunctionCall() {
				replacement := replacementFunc(l.query, term.functionCall)
				if replacement != nil {
					replacedTerms = append(replacedTerms, term)
					result = result + l.query[i:term.functionCall.startIndex] + *replacement
					i = term.functionCall.stopIndex + 1
				}
			}
		}
	}
	result = result + l.query[i:len(l.query)]
	return inspectCqlQuery(result), replacedTerms
}

func (l *cqlListener) replaceNowFunctionCallsWithLiteral(literal uuid.UUID) (QueryInfo, []*term) {
	replacement := literal.String()
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) *string {
		if functionCall.isNow() {
			return &replacement
		} else {
			return nil
		}
	})
}

func (l *cqlListener) replaceNowFunctionCallsWithPositionalBindMarkers() (QueryInfo, []*term) {
	var questionMark = "?"
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) *string {
		if functionCall.isNow() {
			return &questionMark
		} else {
			return nil
		}
	})
}

func (l *cqlListener) replaceNowFunctionCallsWithNamedBindMarkers() (QueryInfo, []*term) {
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) *string {
		if functionCall.isNow() {
			name := fmt.Sprintf(":%s", cloudgateNowNamedMarker)
			return &name
		} else {
			return nil
		}
	})
}

func GetSortedCloudgateNamedMarkers() []string {
	return sortedCloudgateNamedMarkers
}
