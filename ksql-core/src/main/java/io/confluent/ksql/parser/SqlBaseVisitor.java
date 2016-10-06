// Generated from SqlBase.g4 by ANTLR 4.5.3

package io.confluent.ksql.parser;
 
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface SqlBaseVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#statements}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatements(SqlBaseParser.StatementsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatement(SqlBaseParser.StatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#with}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWith(SqlBaseParser.WithContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableProperties}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableProperties(SqlBaseParser.TablePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableProperty}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableProperty(SqlBaseParser.TablePropertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#queryNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryNoWith(SqlBaseParser.QueryNoWithContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable(SqlBaseParser.TableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTable}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTable(SqlBaseParser.InlineTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupBy(SqlBaseParser.GroupByContext ctx);
	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRollup(SqlBaseParser.RollupContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCube(SqlBaseParser.CubeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingExpressions}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingExpressions(SqlBaseParser.GroupingExpressionsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedQuery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedQuery(SqlBaseParser.NamedQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectSingle(SqlBaseParser.SelectSingleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code selectAll}
	 * labeled alternative in {@link SqlBaseParser#selectItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectAll(SqlBaseParser.SelectAllContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelation(SqlBaseParser.RelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinType(SqlBaseParser.JoinTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinCriteria}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sampledRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampledRelation(SqlBaseParser.SampledRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sampleType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampleType(SqlBaseParser.SampleTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#aliasedRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#columnAliases}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnAliases(SqlBaseParser.ColumnAliasesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnnest(SqlBaseParser.UnnestContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanDefault}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#predicated}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicated(SqlBaseParser.PredicatedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBetween(SqlBaseParser.BetweenContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInList(SqlBaseParser.InListContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInSubquery(SqlBaseParser.InSubqueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLike(SqlBaseParser.LikeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullPredicate(SqlBaseParser.NullPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDistinctFrom(SqlBaseParser.DistinctFromContext ctx);
	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcatenation(SqlBaseParser.ConcatenationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtTimeZone(SqlBaseParser.AtTimeZoneContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDereference(SqlBaseParser.DereferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code specialDateTimeFunction}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSpecialDateTimeFunction(SqlBaseParser.SpecialDateTimeFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstring(SqlBaseParser.SubstringContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCast(SqlBaseParser.CastContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLambda(SqlBaseParser.LambdaContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNormalize(SqlBaseParser.NormalizeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowConstructor(SqlBaseParser.RowConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExtract(SqlBaseParser.ExtractContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArrayConstructor(SqlBaseParser.ArrayConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPosition(SqlBaseParser.PositionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timeZoneInterval}
	 * labeled alternative in {@link SqlBaseParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeZoneInterval(SqlBaseParser.TimeZoneIntervalContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timeZoneString}
	 * labeled alternative in {@link SqlBaseParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeZoneString(SqlBaseParser.TimeZoneStringContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#intervalField}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalField(SqlBaseParser.IntervalFieldContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#type}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitType(SqlBaseParser.TypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#typeParameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeParameter(SqlBaseParser.TypeParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#baseType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBaseType(SqlBaseParser.BaseTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#over}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOver(SqlBaseParser.OverContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext ctx);
	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBoundedFrame(SqlBaseParser.BoundedFrameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code explainFormat}
	 * labeled alternative in {@link SqlBaseParser#explainOption}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExplainFormat(SqlBaseParser.ExplainFormatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code explainType}
	 * labeled alternative in {@link SqlBaseParser#explainOption}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExplainType(SqlBaseParser.ExplainTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link SqlBaseParser#transactionMode}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsolationLevel(SqlBaseParser.IsolationLevelContext ctx);
	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link SqlBaseParser#transactionMode}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTransactionAccessMode(SqlBaseParser.TransactionAccessModeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link SqlBaseParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReadUncommitted(SqlBaseParser.ReadUncommittedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link SqlBaseParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReadCommitted(SqlBaseParser.ReadCommittedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link SqlBaseParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatableRead(SqlBaseParser.RepeatableReadContext ctx);
	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link SqlBaseParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSerializable(SqlBaseParser.SerializableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link SqlBaseParser#callArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPositionalArgument(SqlBaseParser.PositionalArgumentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link SqlBaseParser#callArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedArgument(SqlBaseParser.NamedArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#privilege}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrivilege(SqlBaseParser.PrivilegeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code digitIdentifier}
	 * labeled alternative in {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDigitIdentifier(SqlBaseParser.DigitIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(SqlBaseParser.NonReservedContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#normalForm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNormalForm(SqlBaseParser.NormalFormContext ctx);
}