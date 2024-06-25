package zdmproxy

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	parser "github.com/datastax/zdm-proxy/antlr"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
)

type statementType string

type replacementType int

const (
	statementTypeInsert = statementType("insert")
	statementTypeUpdate = statementType("update")
	statementTypeDelete = statementType("delete")
	statementTypeBatch  = statementType("batch")
	statementTypeSelect = statementType("select")
	statementTypeUse    = statementType("use")
	statementTypeOther  = statementType("other")

	zdmNowNamedMarker = "zdm__now"
)

const (
	noReplacement replacementType = iota
	literalReplacement
	namedMarkerReplacement
	positionalMarkerReplacement
)

var (
	sortedZdmNamedMarkers = []string{zdmNowNamedMarker}
	parserPool            = sync.Pool{New: func() interface{} {
		p := parser.NewSimplifiedCqlParser(nil)
		p.RemoveErrorListeners()
		p.SetErrorHandler(antlr.NewBailErrorStrategy())
		p.GetInterpreter().SetPredictionMode(antlr.PredictionModeSLL)
		return p
	}}
	lexerPool = sync.Pool{New: func() interface{} {
		return parser.NewSimplifiedCqlLexer(nil)
	}}
)

type QueryInfo interface {
	getQuery() string
	getStatementType() statementType
	getKeyspaceName() string
	getTableName() string
	isFullyQualified() bool

	// Returns the "current" keyspace when this request was parsed. This could have been set by a "USE" request beforehand
	// or by using the keyspace query/prepare flag in v5 or DseV2.
	getRequestKeyspace() string

	// Returns the keyspace name in the query string if present (getKeyspaceName()). Otherwise, it returns the "current" keyspace
	// when this request was parsed (getRequestKeyspace()).
	getApplicableKeyspace() string

	// Below methods are only relevant for INSERT statements,
	// or BATCH statements containing INSERT statements.

	// Returns a slice of parsedStatement. There is one parsedStatement per statement in the query.
	// For a single INSERT/UPDATE/DELETE, the slice contains only one element. For BATCH statements,
	// the slice will contain as many elements as there are child statements.
	getParsedStatements() []*parsedStatement

	// Returns a parsed select cause object. This is non nil only for intercepted SELECT statements like
	// queries on system.local and system.peers tables.
	getParsedSelectClause() *selectClause

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

	replaceNowFunctionCallsWithLiteral() (QueryInfo, []*term)
	replaceNowFunctionCallsWithPositionalBindMarkers() (QueryInfo, []*term)
	replaceNowFunctionCallsWithNamedBindMarkers() (QueryInfo, []*term)
}

func inspectCqlQuery(query string, currentKeyspace string, timeUuidGenerator TimeUuidGenerator) QueryInfo {
	is := antlr.NewInputStream(query)
	lexer := lexerPool.Get().(*parser.SimplifiedCqlLexer)
	defer lexerPool.Put(lexer)
	lexer.SetInputStream(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	cqlParser := parserPool.Get().(*parser.SimplifiedCqlParser)
	defer parserPool.Put(cqlParser)
	cqlParser.SetInputStream(stream)
	listener := &cqlListener{
		query:             query,
		statementType:     statementTypeOther,
		timeUuidGenerator: timeUuidGenerator,
		requestKeyspace:   currentKeyspace,
	}
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
	return (f.keyspace == "" || f.keyspace == systemKeyspaceName) && f.name == nowFunctionName && f.arity == 0
}

// parsedStatement contains all the information stored by the cqlListener while processing a particular statement.
type parsedStatement struct {
	// The zero-based index of the statement. For single INSERT/UPDATE/DELETE statements, this will be zero. For BATCH child
	// statements, this will be the child index.
	statementIndex int
	statementType  statementType
	terms          []*term
}

func (recv *parsedStatement) ShallowClone() *parsedStatement {
	return &parsedStatement{
		statementIndex: recv.statementIndex,
		statementType:  recv.statementType,
		terms:          recv.terms,
	}
}

type selectClause struct {
	selectors []selector
}

func newStarSelectClause() *selectClause {
	return &selectClause{}
}

func newSelectClauseWithSelectors(selectors []selector) *selectClause {
	return &selectClause{selectors: selectors}
}

func (recv *selectClause) IsStarSelectClause() bool {
	return recv.selectors == nil
}

func (recv *selectClause) GetSelectors() []selector {
	return recv.selectors
}

// selector represents a selector in the cql grammar. 'term' and 'K_CAST' selectors are not supported.
//
//	selector
//	   : unaliasedSelector ( K_AS identifier )?
//	   ;
//
//	unaliasedSelector
//	   : identifier
//	   | term
//	   | K_COUNT '(' '*' ')'
//	   | K_CAST '(' unaliasedSelector K_AS primitiveType ')'
//	   ;
type selector interface {
	Name() string
}

// idSelector represents an unaliased identifier selector:
//
//	unaliasedSelector
//	   : identifier
type idSelector struct {
	name string
}

func (recv *idSelector) Name() string {
	return recv.name
}

// countSelector represents an unaliased count(*) selector:
//
//	K_COUNT '(' '*' ')'
type countSelector struct {
	name string
}

func (recv *countSelector) Name() string {
	return recv.name
}

// aliasedSelector represents an unaliased selector combined with an alias:
//
//	selector
//	   : unaliasedSelector ( K_AS identifier )?
//	   ;
type aliasedSelector struct {
	selector selector
	alias    string
}

func (recv *aliasedSelector) Name() string {
	return recv.alias
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

	// Only filled in for SELECT statements on system.local or system.peers tables
	parsedSelectClause *selectClause

	// Only filled in for INSERT, DELETE, UPDATE and BATCH statements
	parsedStatements      []*parsedStatement
	positionalBindMarkers bool
	namedBindMarkers      bool
	nowFunctionCalls      bool

	// internal counters
	currentPositionalIndex int
	currentBatchChildIndex int

	timeUuidGenerator TimeUuidGenerator

	requestKeyspace string
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

func (l *cqlListener) isFullyQualified() bool {
	return l.getKeyspaceName() != ""
}

func (l *cqlListener) getRequestKeyspace() string {
	return l.requestKeyspace
}

func (l *cqlListener) getApplicableKeyspace() string {
	keyspaceName := l.getKeyspaceName()
	if keyspaceName != "" {
		return keyspaceName
	}
	return l.getRequestKeyspace()
}

func (l *cqlListener) getParsedStatements() []*parsedStatement {
	return l.parsedStatements
}

func (l *cqlListener) getParsedSelectClause() *selectClause {
	return l.parsedSelectClause
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
	if ctx.GetChildCount() == 0 {
		return
	}

	statement := ctx.GetChild(0)
	switch statement.(type) {
	case parser.IInsertStatementContext:
		l.statementType = statementTypeInsert
	case parser.IUpdateStatementContext:
		l.statementType = statementTypeUpdate
	case parser.IDeleteStatementContext:
		l.statementType = statementTypeDelete
	case parser.IBatchStatementContext:
		l.statementType = statementTypeBatch
	case parser.ISelectStatementContext:
		l.statementType = statementTypeSelect
	case parser.IUseStatementContext:
		l.statementType = statementTypeUse
	}
}

func (l *cqlListener) ExitSelectStatement(ctx *parser.SelectStatementContext) {
	if !isSystemKeyspace(l.getApplicableKeyspace()) {
		return
	}

	if !isLocalTable(l.getTableName()) && !isPeersV1Table(l.getTableName()) && !isPeersV2Table(l.getTableName()) {
		return
	}

	for i := 0; i < ctx.GetChildCount(); i++ {
		child := ctx.GetChild(i)
		if child == nil {
			break
		}
		switch typedChild := child.(type) {
		case antlr.TerminalNode:
			if typedChild.GetSymbol().GetTokenType() == parser.SimplifiedCqlParserK_JSON ||
				typedChild.GetSymbol().GetTokenType() == parser.SimplifiedCqlParserK_DISTINCT {
				log.Warnf("Proxy does not support 'JSON' or 'DISTINCT' for system.local and system.peers queries: %v", ctx.GetText())
				return
			}
		case *parser.SelectClauseContext:
			parsedSelectClause, err := extractSelectClause(typedChild)
			if err != nil {
				log.Warnf("Proxy could not parse select clause of system.local/system.peers query: %v", err.Error())
				return
			}
			l.parsedSelectClause = parsedSelectClause
			return
		default:
			log.Errorf("Proxy could not parse SELECT query for system.local/peers: %v", ctx.GetText())
			return
		}
	}
}

func (l *cqlListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	parsedStmt := &parsedStatement{statementIndex: l.currentBatchChildIndex, statementType: statementTypeInsert}
	for _, childCtx := range ctx.GetChildren() {
		switch childCtx.(type) {
		case parser.ITermsContext:
			parsedStmt.terms = append(parsedStmt.terms, l.extractTerms(childCtx)...)
		case parser.IUsingClauseContext:
			parsedStmt.terms = append(parsedStmt.terms, l.extractUsingClauseBindMarkers(childCtx)...)
		}
	}

	l.parsedStatements = append(l.parsedStatements, parsedStmt)
	l.currentBatchChildIndex++
}

func (l *cqlListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	parsedStmt := &parsedStatement{statementIndex: l.currentBatchChildIndex, statementType: statementTypeUpdate}

	for _, childCtx := range ctx.GetChildren() {
		switch childCtx.(type) {
		case parser.IUsingClauseContext:
			parsedStmt.terms = append(parsedStmt.terms, l.extractUsingClauseBindMarkers(childCtx)...)
		case parser.IUpdateOperationsContext:
			for _, updateOperation := range childCtx.GetChildren() {
				for _, termCtx := range updateOperation.GetChildren() {
					typedTermCtx, ok := termCtx.(*parser.TermContext)
					if ok {
						parsedStmt.terms = append(parsedStmt.terms, l.extractTerm(typedTermCtx))
					}
				}
			}
		case parser.IWhereClauseContext:
			whereClauseTerms := l.extractWhereClauseTerms(childCtx)
			parsedStmt.terms = append(parsedStmt.terms, whereClauseTerms...)
		case parser.IConditionsContext:
			conditionTerms := l.extractConditionsTerms(childCtx)
			parsedStmt.terms = append(parsedStmt.terms, conditionTerms...)
		}
	}

	l.parsedStatements = append(l.parsedStatements, parsedStmt)
	l.currentBatchChildIndex++
}

func (l *cqlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	parsedStmt := &parsedStatement{statementIndex: l.currentBatchChildIndex, statementType: statementTypeDelete}

	for _, childCtx := range ctx.GetChildren() {
		switch childCtx.(type) {
		case parser.IDeleteOperationsContext:
			for _, deleteOperation := range childCtx.GetChildren() {
				deleteOperationTyped, ok := deleteOperation.(*parser.DeleteOperationContext)
				if ok {
					t := deleteOperationTyped.Term()
					if t != nil {
						parsedStmt.terms = append(parsedStmt.terms, l.extractTerm(t))
					}
				}
			}
		case parser.ITimestampContext:
			parsedTimestampCtx := childCtx.(*parser.TimestampContext)
			timeStampTerm := l.extractNillableBindMarker(parsedTimestampCtx.BindMarker())
			if timeStampTerm != nil {
				parsedStmt.terms = append(parsedStmt.terms, timeStampTerm)
			}
		case parser.IWhereClauseContext:
			whereClauseTerms := l.extractWhereClauseTerms(childCtx)
			parsedStmt.terms = append(parsedStmt.terms, whereClauseTerms...)
		case parser.IConditionsContext:
			conditionTerms := l.extractConditionsTerms(childCtx)
			parsedStmt.terms = append(parsedStmt.terms, conditionTerms...)
		}
	}

	l.parsedStatements = append(l.parsedStatements, parsedStmt)
	l.currentBatchChildIndex++
}

func (l *cqlListener) EnterBatchStatement(ctx *parser.BatchStatementContext) {
	usingClauseCtx := ctx.UsingClause()
	if usingClauseCtx != nil {
		// ignore terms, just process the clause to update the current positional marker position that is used in the actual child statements
		_ = l.extractUsingClauseBindMarkers(usingClauseCtx)
	}
}

func (l *cqlListener) EnterUseStatement(ctx *parser.UseStatementContext) {
	l.keyspaceName = extractIdentifier(ctx.KeyspaceName().(*parser.KeyspaceNameContext).Identifier().(*parser.IdentifierContext))
}

func (l *cqlListener) EnterTableName(ctx *parser.TableNameContext) {
	qualifiedId := ctx.GetChild(0)
	// Note: this will capture the *last* table name in a BATCH statement
	if qualifiedId.GetChildCount() == 1 {
		identifierContext := qualifiedId.GetChild(0).(*parser.IdentifierContext)
		l.tableName = extractIdentifier(identifierContext)
	} else {
		// 3 children: keyspaceName, token DOT, identifier
		keyspaceNameContext := qualifiedId.GetChild(0)
		l.keyspaceName = extractIdentifier(keyspaceNameContext.GetChild(0).(*parser.IdentifierContext))
		identifierContext := qualifiedId.GetChild(2).(*parser.IdentifierContext)
		l.tableName = extractIdentifier(identifierContext)
	}
}

func extractSelectClause(selectClauseCtx *parser.SelectClauseContext) (*selectClause, error) {
	child := selectClauseCtx.GetChild(0)
	switch typedChild := child.(type) {
	case antlr.TerminalNode:
		return newStarSelectClause(), nil
	case *parser.SelectorsContext:
		selectors, err := extractSelectors(typedChild)
		if err != nil {
			return nil, err
		}
		return newSelectClauseWithSelectors(selectors), nil
	}
	return nil, fmt.Errorf("unexpected select clause: %v", selectClauseCtx.GetText())
}

func extractSelectors(selectorsCtx *parser.SelectorsContext) ([]selector, error) {
	selectors := make([]selector, 0)
	for i := 0; i < selectorsCtx.GetChildCount(); i++ {
		child := selectorsCtx.GetChild(i)
		switch typedChild := child.(type) {
		case *parser.SelectorContext:
			parsedSelector, err := extractSelector(typedChild)
			if err != nil {
				return nil, err
			}
			selectors = append(selectors, parsedSelector)
		default:
		}
	}
	return selectors, nil
}

func extractSelector(selectorCtx *parser.SelectorContext) (selector, error) {
	var parsedSelector selector
	unaliasedSelector := selectorCtx.GetChild(0).(*parser.UnaliasedSelectorContext)
	switch unaliasedSelectorChild := unaliasedSelector.GetChild(0).(type) {
	case *parser.IdentifierContext:
		parsedSelector = &idSelector{name: extractIdentifier(unaliasedSelectorChild)}
	case *parser.TermContext:
		return nil, fmt.Errorf("term selector (%v) not supported", unaliasedSelectorChild.GetText())
	case antlr.TerminalNode:
		if unaliasedSelectorChild.GetSymbol().GetTokenType() == parser.SimplifiedCqlParserK_COUNT {
			parsedSelector = &countSelector{name: unaliasedSelectorChild.GetText()}
		} else {
			return nil, fmt.Errorf("unrecognized terminal node when parsing selector (%v)", unaliasedSelectorChild.GetText())
		}
	}
	if selectorCtx.GetChildCount() == 3 {
		return &aliasedSelector{
			selector: parsedSelector,
			alias:    extractIdentifier(selectorCtx.GetChild(2).(*parser.IdentifierContext)),
		}, nil
	} else {
		return parsedSelector, nil
	}
}

func (l *cqlListener) extractAllTerms(termsCtx []antlr.Tree) []*term {
	var terms []*term
	for _, termCtx := range termsCtx {
		typedTermCtx, ok := termCtx.(*parser.TermContext)
		if ok {
			terms = append(terms, l.extractTerm(typedTermCtx))
		}
	}
	return terms
}

func (l *cqlListener) extractTerms(termsCtx antlr.Tree) []*term {
	return l.extractAllTerms(termsCtx.GetChildren())
}

func (l *cqlListener) extractTerm(termCtx antlr.Tree) *term {
	for _, childCtx := range termCtx.GetChildren() {
		switch typedCtx := childCtx.(type) {
		case parser.ITypeCastContext:
			return l.extractTerm(childCtx.GetChild(3))
		case parser.ILiteralContext:
			return NewLiteralTerm(typedCtx.GetText(), l.currentPositionalIndex-1)
		case parser.IFunctionCallContext:
			fCall := extractFunctionCall(childCtx.(*parser.FunctionCallContext))
			if fCall.isNow() {
				l.nowFunctionCalls = true
			}
			return NewFunctionCallTerm(fCall, l.currentPositionalIndex-1)
		case parser.IBindMarkerContext:
			return l.extractBindMarker(childCtx)
		}
	}
	return nil
}

func (l *cqlListener) extractAllBindMarkers(allBindMarkersCtx []antlr.Tree) []*term {
	var terms []*term
	for _, bindMarkerCtx := range allBindMarkersCtx {
		typedBindMarkerCtx, ok := bindMarkerCtx.(*parser.BindMarkerContext)
		if ok {
			terms = append(terms, l.extractBindMarker(typedBindMarkerCtx))
		}
	}
	return terms
}

func (l *cqlListener) extractBindMarkers(bindMarkersCtx antlr.Tree) []*term {
	return l.extractAllBindMarkers(bindMarkersCtx.GetChildren())
}

func (l *cqlListener) extractBindMarker(bindMarkerCtx antlr.Tree) *term {
	for _, childCtx := range bindMarkerCtx.GetChildren() {
		switch childCtx.(type) {
		case parser.IPositionalBindMarkerContext:
			l.positionalBindMarkers = true
			newTerm := NewPositionalBindMarkerTerm(l.currentPositionalIndex)
			l.currentPositionalIndex++
			return newTerm
		case parser.INamedBindMarkerContext:
			l.namedBindMarkers = true
			bindMarkerName := extractIdentifier(childCtx.GetChild(1).(*parser.IdentifierContext))
			return NewNamedBindMarkerTerm(bindMarkerName, l.currentPositionalIndex-1)
		}
	}

	log.Errorf("Could not parse bind marker: %T", bindMarkerCtx)
	return nil
}

func (l *cqlListener) extractWhereClauseTerms(ctx antlr.Tree) []*term {
	var terms []*term

	for _, relationCtx := range ctx.GetChildren() {
		relationTyped, ok := relationCtx.(*parser.RelationContext)
		if ok {
			terms = append(terms, l.extractRelationTerms(relationTyped)...)
		}
	}

	return terms
}

func (l *cqlListener) extractRelationTerms(ctx antlr.Tree) []*term {
	terms := make([]*term, 0)
	for _, childCtx := range ctx.GetChildren() {
		switch childCtx.(type) {
		case parser.IRelationContext:
			return l.extractRelationTerms(childCtx)
		case parser.ITermsContext:
			terms = append(terms, l.extractAllTerms(childCtx.GetChildren())...)
		case parser.ITermContext:
			terms = append(terms, l.extractTerm(childCtx))
		case parser.IBindMarkersContext:
			terms = append(terms, l.extractBindMarkers(childCtx)...)
		case parser.IBindMarkerContext:
			terms = append(terms, l.extractBindMarker(childCtx))
		case parser.ITupleLiteralContext:
			termsCtx := childCtx.GetChild(1)
			terms = append(terms, l.extractTerms(termsCtx)...)
		case parser.ITupleLiteralsContext:
			for _, tupleLiteralCtx := range childCtx.GetChildren() {
				typedTupleLiteralCtx, ok := tupleLiteralCtx.(*parser.TupleLiteralContext)
				if ok {
					terms = append(terms, l.extractTerms(typedTupleLiteralCtx.GetChild(1))...)
				}
			}
		}
	}

	return terms
}

func (l *cqlListener) extractConditionsTerms(ctx antlr.Tree) []*term {
	var terms []*term

	for _, conditionCtx := range ctx.GetChildren() {
		for _, childCtx := range conditionCtx.GetChildren() {
			switch childCtx.(type) {
			case parser.ITermContext:
				terms = append(terms, l.extractTerm(childCtx))
			case parser.ITermsContext:
				terms = append(terms, l.extractTerms(childCtx)...)
			case parser.IBindMarkerContext:
				terms = append(terms, l.extractBindMarker(childCtx))
			}
		}
	}

	return terms
}

func (l *cqlListener) extractUsingClauseBindMarkers(ctx antlr.Tree) []*term {
	var terms []*term

	for _, childCtx := range ctx.GetChildren() {
		var bindMarkerTerm *term
		switch childCtx.(type) {
		case parser.ITimestampContext, parser.ITtlContext:
			bindMarkerTerm = l.extractNillableBindMarker(childCtx.GetChild(1))
		}

		if bindMarkerTerm != nil {
			terms = append(terms, bindMarkerTerm)
		}
	}

	return terms
}

func (l *cqlListener) extractNillableBindMarker(ctx antlr.Tree) *term {
	if ctx != nil {
		typedCtx, ok := ctx.(*parser.BindMarkerContext)
		if ok {
			return l.extractBindMarker(typedCtx)
		}
	}

	return nil
}

func extractFunctionCall(ctx *parser.FunctionCallContext) *functionCall {
	qualifiedIdentifierCtx := ctx.GetChild(0).GetChild(0).(*parser.QualifiedIdentifierContext)
	keyspaceName := ""
	functionNameChildIdx := 0
	if qualifiedIdentifierCtx.GetChildCount() > 1 {
		keyspaceName = extractIdentifier(qualifiedIdentifierCtx.GetChild(0).GetChild(0).(*parser.IdentifierContext))
		functionNameChildIdx = 2
	}
	functionName := extractIdentifier(qualifiedIdentifierCtx.GetChild(functionNameChildIdx).(*parser.IdentifierContext))
	// For now we only record the function arity, not the actual function arguments
	functionArity := 0
	if ctx.GetChildCount() == 4 {
		functionArity = ctx.GetChild(2).GetChildCount()
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
	childCtx := identifierContext.GetChild(0)
	switch typedChildCtx := childCtx.(type) {
	case antlr.TerminalNode:
		switch typedChildCtx.GetSymbol().GetTokenType() {
		case parser.SimplifiedCqlParserQUOTED_IDENTIFIER:
			identifier := typedChildCtx.GetText()
			// remove surrounding quotes
			identifier = identifier[1 : len(identifier)-1]
			// handle escaped double-quotes
			identifier = strings.ReplaceAll(identifier, "\"\"", "\"")
			return identifier
		default: // UNQUOTED
			return strings.ToLower(typedChildCtx.GetText())
		}
	default: // UNRESERVED KEYWORD
		return strings.ToLower(childCtx.(*parser.UnreservedKeywordContext).GetText())
	}
}

func (l *cqlListener) replaceFunctionCalls(replacementFunc func(query string, functionCall *functionCall) (string, replacementType)) (QueryInfo, []*term) {
	if !l.hasNowFunctionCalls() {
		return l, make([]*term, 0)
	}
	var result string
	i := 0
	replacedTerms := make([]*term, 0)
	newParsedStatements := make([]*parsedStatement, 0, len(l.parsedStatements))
	previousPositionalIndex := 0
	namedMarkers := false
	positionalMarkers := false
	for _, parsedStmt := range l.parsedStatements {
		newParsedStmt := parsedStmt.ShallowClone()
		newTerms := make([]*term, 0)
		for _, t := range parsedStmt.terms {
			var newTerm *term
			if t.isFunctionCall() {
				replacement, rType := replacementFunc(l.query, t.functionCall)
				if rType != noReplacement {
					replacedTerms = append(replacedTerms, t)
					result = result + l.query[i:t.functionCall.startIndex] + replacement
					i = t.functionCall.stopIndex + 1
					switch rType {
					case literalReplacement:
						newTerm = NewLiteralTerm(replacement, t.previousPositionalIndex)
					case namedMarkerReplacement:
						newTerm = NewNamedBindMarkerTerm(replacement[1:], t.previousPositionalIndex)
					case positionalMarkerReplacement:
						newTerm = NewPositionalBindMarkerTerm(previousPositionalIndex + 1)
						previousPositionalIndex++
					}
				}
			}
			if newTerm == nil {
				newTerm = t
			}
			newTerms = append(newTerms, newTerm)
			if newTerm.isPositionalBindMarker() {
				positionalMarkers = true
			} else if newTerm.isNamedBindMarker() {
				namedMarkers = true
			}
		}
		newParsedStmt.terms = newTerms
		newParsedStatements = append(newParsedStatements, newParsedStmt)
	}
	result = result + l.query[i:len(l.query)]
	newQueryInfo := l.shallowClone()
	newQueryInfo.query = result
	newQueryInfo.nowFunctionCalls = false
	newQueryInfo.parsedStatements = newParsedStatements
	newQueryInfo.namedBindMarkers = namedMarkers
	newQueryInfo.positionalBindMarkers = positionalMarkers
	return newQueryInfo, replacedTerms
}

func (l *cqlListener) replaceNowFunctionCallsWithLiteral() (QueryInfo, []*term) {
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) (string, replacementType) {
		if functionCall.isNow() {
			return l.timeUuidGenerator.GetTimeUuid().String(), literalReplacement
		} else {
			return "", noReplacement
		}
	})
}

func (l *cqlListener) replaceNowFunctionCallsWithPositionalBindMarkers() (QueryInfo, []*term) {
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) (string, replacementType) {
		if functionCall.isNow() {
			return "?", positionalMarkerReplacement
		} else {
			return "", noReplacement
		}
	})
}

func (l *cqlListener) replaceNowFunctionCallsWithNamedBindMarkers() (QueryInfo, []*term) {
	return l.replaceFunctionCalls(func(query string, functionCall *functionCall) (string, replacementType) {
		if functionCall.isNow() {
			return fmt.Sprintf(":%s", zdmNowNamedMarker), namedMarkerReplacement
		} else {
			return "", noReplacement
		}
	})
}

func (l *cqlListener) shallowClone() *cqlListener {
	return &cqlListener{
		BaseSimplifiedCqlListener: l.BaseSimplifiedCqlListener,
		query:                     l.query,
		statementType:             l.statementType,
		keyspaceName:              l.keyspaceName,
		tableName:                 l.tableName,
		parsedStatements:          l.parsedStatements,
		positionalBindMarkers:     l.positionalBindMarkers,
		namedBindMarkers:          l.namedBindMarkers,
		nowFunctionCalls:          l.nowFunctionCalls,
		currentPositionalIndex:    l.currentPositionalIndex,
		currentBatchChildIndex:    l.currentBatchChildIndex,
		timeUuidGenerator:         l.timeUuidGenerator,
		requestKeyspace:           l.requestKeyspace,
		parsedSelectClause:        l.parsedSelectClause,
	}
}

func GetSortedZdmNamedMarkers() []string {
	return sortedZdmNamedMarkers
}
