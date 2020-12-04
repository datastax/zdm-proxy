// Code generated from antlr/SimplifiedCql.g4 by ANTLR 4.8. DO NOT EDIT.

package parser // SimplifiedCql

import "github.com/antlr/antlr4/runtime/Go/antlr"

// BaseSimplifiedCqlListener is a complete listener for a parse tree produced by SimplifiedCqlParser.
type BaseSimplifiedCqlListener struct{}

var _ SimplifiedCqlListener = &BaseSimplifiedCqlListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseSimplifiedCqlListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseSimplifiedCqlListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseSimplifiedCqlListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseSimplifiedCqlListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterCqlStatement is called when production cqlStatement is entered.
func (s *BaseSimplifiedCqlListener) EnterCqlStatement(ctx *CqlStatementContext) {}

// ExitCqlStatement is called when production cqlStatement is exited.
func (s *BaseSimplifiedCqlListener) ExitCqlStatement(ctx *CqlStatementContext) {}

// EnterSelectStatement is called when production selectStatement is entered.
func (s *BaseSimplifiedCqlListener) EnterSelectStatement(ctx *SelectStatementContext) {}

// ExitSelectStatement is called when production selectStatement is exited.
func (s *BaseSimplifiedCqlListener) ExitSelectStatement(ctx *SelectStatementContext) {}

// EnterOtherStatement is called when production otherStatement is entered.
func (s *BaseSimplifiedCqlListener) EnterOtherStatement(ctx *OtherStatementContext) {}

// ExitOtherStatement is called when production otherStatement is exited.
func (s *BaseSimplifiedCqlListener) ExitOtherStatement(ctx *OtherStatementContext) {}

// EnterSelectClause is called when production selectClause is entered.
func (s *BaseSimplifiedCqlListener) EnterSelectClause(ctx *SelectClauseContext) {}

// ExitSelectClause is called when production selectClause is exited.
func (s *BaseSimplifiedCqlListener) ExitSelectClause(ctx *SelectClauseContext) {}

// EnterSelector is called when production selector is entered.
func (s *BaseSimplifiedCqlListener) EnterSelector(ctx *SelectorContext) {}

// ExitSelector is called when production selector is exited.
func (s *BaseSimplifiedCqlListener) ExitSelector(ctx *SelectorContext) {}

// EnterUnaliasedSelector is called when production unaliasedSelector is entered.
func (s *BaseSimplifiedCqlListener) EnterUnaliasedSelector(ctx *UnaliasedSelectorContext) {}

// ExitUnaliasedSelector is called when production unaliasedSelector is exited.
func (s *BaseSimplifiedCqlListener) ExitUnaliasedSelector(ctx *UnaliasedSelectorContext) {}

// EnterTerm is called when production term is entered.
func (s *BaseSimplifiedCqlListener) EnterTerm(ctx *TermContext) {}

// ExitTerm is called when production term is exited.
func (s *BaseSimplifiedCqlListener) ExitTerm(ctx *TermContext) {}

// EnterLiteral is called when production literal is entered.
func (s *BaseSimplifiedCqlListener) EnterLiteral(ctx *LiteralContext) {}

// ExitLiteral is called when production literal is exited.
func (s *BaseSimplifiedCqlListener) ExitLiteral(ctx *LiteralContext) {}

// EnterPrimitiveLiteral is called when production primitiveLiteral is entered.
func (s *BaseSimplifiedCqlListener) EnterPrimitiveLiteral(ctx *PrimitiveLiteralContext) {}

// ExitPrimitiveLiteral is called when production primitiveLiteral is exited.
func (s *BaseSimplifiedCqlListener) ExitPrimitiveLiteral(ctx *PrimitiveLiteralContext) {}

// EnterCollectionLiteral is called when production collectionLiteral is entered.
func (s *BaseSimplifiedCqlListener) EnterCollectionLiteral(ctx *CollectionLiteralContext) {}

// ExitCollectionLiteral is called when production collectionLiteral is exited.
func (s *BaseSimplifiedCqlListener) ExitCollectionLiteral(ctx *CollectionLiteralContext) {}

// EnterListLiteral is called when production listLiteral is entered.
func (s *BaseSimplifiedCqlListener) EnterListLiteral(ctx *ListLiteralContext) {}

// ExitListLiteral is called when production listLiteral is exited.
func (s *BaseSimplifiedCqlListener) ExitListLiteral(ctx *ListLiteralContext) {}

// EnterSetLiteral is called when production setLiteral is entered.
func (s *BaseSimplifiedCqlListener) EnterSetLiteral(ctx *SetLiteralContext) {}

// ExitSetLiteral is called when production setLiteral is exited.
func (s *BaseSimplifiedCqlListener) ExitSetLiteral(ctx *SetLiteralContext) {}

// EnterMapLiteral is called when production mapLiteral is entered.
func (s *BaseSimplifiedCqlListener) EnterMapLiteral(ctx *MapLiteralContext) {}

// ExitMapLiteral is called when production mapLiteral is exited.
func (s *BaseSimplifiedCqlListener) ExitMapLiteral(ctx *MapLiteralContext) {}

// EnterCqlType is called when production cqlType is entered.
func (s *BaseSimplifiedCqlListener) EnterCqlType(ctx *CqlTypeContext) {}

// ExitCqlType is called when production cqlType is exited.
func (s *BaseSimplifiedCqlListener) ExitCqlType(ctx *CqlTypeContext) {}

// EnterPrimitiveType is called when production primitiveType is entered.
func (s *BaseSimplifiedCqlListener) EnterPrimitiveType(ctx *PrimitiveTypeContext) {}

// ExitPrimitiveType is called when production primitiveType is exited.
func (s *BaseSimplifiedCqlListener) ExitPrimitiveType(ctx *PrimitiveTypeContext) {}

// EnterCollectionType is called when production collectionType is entered.
func (s *BaseSimplifiedCqlListener) EnterCollectionType(ctx *CollectionTypeContext) {}

// ExitCollectionType is called when production collectionType is exited.
func (s *BaseSimplifiedCqlListener) ExitCollectionType(ctx *CollectionTypeContext) {}

// EnterTupleType is called when production tupleType is entered.
func (s *BaseSimplifiedCqlListener) EnterTupleType(ctx *TupleTypeContext) {}

// ExitTupleType is called when production tupleType is exited.
func (s *BaseSimplifiedCqlListener) ExitTupleType(ctx *TupleTypeContext) {}

// EnterFunctionCall is called when production functionCall is entered.
func (s *BaseSimplifiedCqlListener) EnterFunctionCall(ctx *FunctionCallContext) {}

// ExitFunctionCall is called when production functionCall is exited.
func (s *BaseSimplifiedCqlListener) ExitFunctionCall(ctx *FunctionCallContext) {}

// EnterFunctionArgs is called when production functionArgs is entered.
func (s *BaseSimplifiedCqlListener) EnterFunctionArgs(ctx *FunctionArgsContext) {}

// ExitFunctionArgs is called when production functionArgs is exited.
func (s *BaseSimplifiedCqlListener) ExitFunctionArgs(ctx *FunctionArgsContext) {}

// EnterTableName is called when production tableName is entered.
func (s *BaseSimplifiedCqlListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseSimplifiedCqlListener) ExitTableName(ctx *TableNameContext) {}

// EnterFunctionName is called when production functionName is entered.
func (s *BaseSimplifiedCqlListener) EnterFunctionName(ctx *FunctionNameContext) {}

// ExitFunctionName is called when production functionName is exited.
func (s *BaseSimplifiedCqlListener) ExitFunctionName(ctx *FunctionNameContext) {}

// EnterUserTypeName is called when production userTypeName is entered.
func (s *BaseSimplifiedCqlListener) EnterUserTypeName(ctx *UserTypeNameContext) {}

// ExitUserTypeName is called when production userTypeName is exited.
func (s *BaseSimplifiedCqlListener) ExitUserTypeName(ctx *UserTypeNameContext) {}

// EnterKeyspaceName is called when production keyspaceName is entered.
func (s *BaseSimplifiedCqlListener) EnterKeyspaceName(ctx *KeyspaceNameContext) {}

// ExitKeyspaceName is called when production keyspaceName is exited.
func (s *BaseSimplifiedCqlListener) ExitKeyspaceName(ctx *KeyspaceNameContext) {}

// EnterQualifiedIdentifier is called when production qualifiedIdentifier is entered.
func (s *BaseSimplifiedCqlListener) EnterQualifiedIdentifier(ctx *QualifiedIdentifierContext) {}

// ExitQualifiedIdentifier is called when production qualifiedIdentifier is exited.
func (s *BaseSimplifiedCqlListener) ExitQualifiedIdentifier(ctx *QualifiedIdentifierContext) {}

// EnterIdentifier is called when production identifier is entered.
func (s *BaseSimplifiedCqlListener) EnterIdentifier(ctx *IdentifierContext) {}

// ExitIdentifier is called when production identifier is exited.
func (s *BaseSimplifiedCqlListener) ExitIdentifier(ctx *IdentifierContext) {}

// EnterDiscardedContent is called when production discardedContent is entered.
func (s *BaseSimplifiedCqlListener) EnterDiscardedContent(ctx *DiscardedContentContext) {}

// ExitDiscardedContent is called when production discardedContent is exited.
func (s *BaseSimplifiedCqlListener) ExitDiscardedContent(ctx *DiscardedContentContext) {}

// EnterUnknown is called when production unknown is entered.
func (s *BaseSimplifiedCqlListener) EnterUnknown(ctx *UnknownContext) {}

// ExitUnknown is called when production unknown is exited.
func (s *BaseSimplifiedCqlListener) ExitUnknown(ctx *UnknownContext) {}
