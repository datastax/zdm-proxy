// Code generated from antlr/SimplifiedCql.g4 by ANTLR 4.8. DO NOT EDIT.

package parser // SimplifiedCql

import "github.com/antlr/antlr4/runtime/Go/antlr"

// SimplifiedCqlListener is a complete listener for a parse tree produced by SimplifiedCqlParser.
type SimplifiedCqlListener interface {
	antlr.ParseTreeListener

	// EnterCqlStatement is called when entering the cqlStatement production.
	EnterCqlStatement(c *CqlStatementContext)

	// EnterSelectStatement is called when entering the selectStatement production.
	EnterSelectStatement(c *SelectStatementContext)

	// EnterOtherStatement is called when entering the otherStatement production.
	EnterOtherStatement(c *OtherStatementContext)

	// EnterSelectClause is called when entering the selectClause production.
	EnterSelectClause(c *SelectClauseContext)

	// EnterSelector is called when entering the selector production.
	EnterSelector(c *SelectorContext)

	// EnterUnaliasedSelector is called when entering the unaliasedSelector production.
	EnterUnaliasedSelector(c *UnaliasedSelectorContext)

	// EnterTerm is called when entering the term production.
	EnterTerm(c *TermContext)

	// EnterLiteral is called when entering the literal production.
	EnterLiteral(c *LiteralContext)

	// EnterPrimitiveLiteral is called when entering the primitiveLiteral production.
	EnterPrimitiveLiteral(c *PrimitiveLiteralContext)

	// EnterCollectionLiteral is called when entering the collectionLiteral production.
	EnterCollectionLiteral(c *CollectionLiteralContext)

	// EnterListLiteral is called when entering the listLiteral production.
	EnterListLiteral(c *ListLiteralContext)

	// EnterSetLiteral is called when entering the setLiteral production.
	EnterSetLiteral(c *SetLiteralContext)

	// EnterMapLiteral is called when entering the mapLiteral production.
	EnterMapLiteral(c *MapLiteralContext)

	// EnterCqlType is called when entering the cqlType production.
	EnterCqlType(c *CqlTypeContext)

	// EnterPrimitiveType is called when entering the primitiveType production.
	EnterPrimitiveType(c *PrimitiveTypeContext)

	// EnterCollectionType is called when entering the collectionType production.
	EnterCollectionType(c *CollectionTypeContext)

	// EnterTupleType is called when entering the tupleType production.
	EnterTupleType(c *TupleTypeContext)

	// EnterFunctionCall is called when entering the functionCall production.
	EnterFunctionCall(c *FunctionCallContext)

	// EnterFunctionArgs is called when entering the functionArgs production.
	EnterFunctionArgs(c *FunctionArgsContext)

	// EnterTableName is called when entering the tableName production.
	EnterTableName(c *TableNameContext)

	// EnterFunctionName is called when entering the functionName production.
	EnterFunctionName(c *FunctionNameContext)

	// EnterUserTypeName is called when entering the userTypeName production.
	EnterUserTypeName(c *UserTypeNameContext)

	// EnterKeyspaceName is called when entering the keyspaceName production.
	EnterKeyspaceName(c *KeyspaceNameContext)

	// EnterQualifiedIdentifier is called when entering the qualifiedIdentifier production.
	EnterQualifiedIdentifier(c *QualifiedIdentifierContext)

	// EnterIdentifier is called when entering the identifier production.
	EnterIdentifier(c *IdentifierContext)

	// EnterDiscardedContent is called when entering the discardedContent production.
	EnterDiscardedContent(c *DiscardedContentContext)

	// EnterUnknown is called when entering the unknown production.
	EnterUnknown(c *UnknownContext)

	// ExitCqlStatement is called when exiting the cqlStatement production.
	ExitCqlStatement(c *CqlStatementContext)

	// ExitSelectStatement is called when exiting the selectStatement production.
	ExitSelectStatement(c *SelectStatementContext)

	// ExitOtherStatement is called when exiting the otherStatement production.
	ExitOtherStatement(c *OtherStatementContext)

	// ExitSelectClause is called when exiting the selectClause production.
	ExitSelectClause(c *SelectClauseContext)

	// ExitSelector is called when exiting the selector production.
	ExitSelector(c *SelectorContext)

	// ExitUnaliasedSelector is called when exiting the unaliasedSelector production.
	ExitUnaliasedSelector(c *UnaliasedSelectorContext)

	// ExitTerm is called when exiting the term production.
	ExitTerm(c *TermContext)

	// ExitLiteral is called when exiting the literal production.
	ExitLiteral(c *LiteralContext)

	// ExitPrimitiveLiteral is called when exiting the primitiveLiteral production.
	ExitPrimitiveLiteral(c *PrimitiveLiteralContext)

	// ExitCollectionLiteral is called when exiting the collectionLiteral production.
	ExitCollectionLiteral(c *CollectionLiteralContext)

	// ExitListLiteral is called when exiting the listLiteral production.
	ExitListLiteral(c *ListLiteralContext)

	// ExitSetLiteral is called when exiting the setLiteral production.
	ExitSetLiteral(c *SetLiteralContext)

	// ExitMapLiteral is called when exiting the mapLiteral production.
	ExitMapLiteral(c *MapLiteralContext)

	// ExitCqlType is called when exiting the cqlType production.
	ExitCqlType(c *CqlTypeContext)

	// ExitPrimitiveType is called when exiting the primitiveType production.
	ExitPrimitiveType(c *PrimitiveTypeContext)

	// ExitCollectionType is called when exiting the collectionType production.
	ExitCollectionType(c *CollectionTypeContext)

	// ExitTupleType is called when exiting the tupleType production.
	ExitTupleType(c *TupleTypeContext)

	// ExitFunctionCall is called when exiting the functionCall production.
	ExitFunctionCall(c *FunctionCallContext)

	// ExitFunctionArgs is called when exiting the functionArgs production.
	ExitFunctionArgs(c *FunctionArgsContext)

	// ExitTableName is called when exiting the tableName production.
	ExitTableName(c *TableNameContext)

	// ExitFunctionName is called when exiting the functionName production.
	ExitFunctionName(c *FunctionNameContext)

	// ExitUserTypeName is called when exiting the userTypeName production.
	ExitUserTypeName(c *UserTypeNameContext)

	// ExitKeyspaceName is called when exiting the keyspaceName production.
	ExitKeyspaceName(c *KeyspaceNameContext)

	// ExitQualifiedIdentifier is called when exiting the qualifiedIdentifier production.
	ExitQualifiedIdentifier(c *QualifiedIdentifierContext)

	// ExitIdentifier is called when exiting the identifier production.
	ExitIdentifier(c *IdentifierContext)

	// ExitDiscardedContent is called when exiting the discardedContent production.
	ExitDiscardedContent(c *DiscardedContentContext)

	// ExitUnknown is called when exiting the unknown production.
	ExitUnknown(c *UnknownContext)
}
