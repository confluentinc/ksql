/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.ksql.parser.tree;

import javax.annotation.Nullable;

public abstract class DefaultASTVisitor<R, C>
    extends AstVisitor<R, C> {

  public R process(Node node, @Nullable C context) {
    return node.accept(this, context);
  }

  protected R visitNode(Node node, C context) {
    return null;
  }

  protected R visitExpression(Expression node, C context) {
    return visitNode(node, context);
  }

  protected R visitExtract(Extract node, C context) {
    return visitExpression(node, context);
  }

  protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return visitExpression(node, context);
  }

  protected R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitComparisonExpression(ComparisonExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return visitExpression(node, context);
  }

  protected R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDoubleLiteral(DoubleLiteral node, C context) {
    return null;
  }

  protected R visitDecimalLiteral(DecimalLiteral node, C context) {
    return null;
  }

  protected R visitStatements(Statements node, C context) {
    return visitNode(node, context);
  }

  protected R visitStatement(Statement node, C context) {
    return visitNode(node, context);
  }

  protected R visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  protected R visitExplain(Explain node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowSchemas(ShowSchemas node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowCatalogs(ShowCatalogs node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowColumns(ShowColumns node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowPartitions(ShowPartitions node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowCreate(ShowCreate node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowFunctions(ShowFunctions node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowSession(ShowSession node, C context) {
    return visitStatement(node, context);
  }

  protected R visitSetSession(SetSession node, C context) {
    return visitStatement(node, context);
  }

  protected R visitGenericLiteral(GenericLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitTimeLiteral(TimeLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitExplainOption(ExplainOption node, C context) {
    return visitNode(node, context);
  }

  protected R visitWith(With node, C context) {
    return visitNode(node, context);
  }

  protected R visitWithQuery(WithQuery node, C context) {
    return visitNode(node, context);
  }

  protected R visitSelect(Select node, C context) {
    return visitNode(node, context);
  }

  protected R visitRelation(Relation node, C context) {
    return visitNode(node, context);
  }

  protected R visitQueryBody(QueryBody node, C context) {
    return visitRelation(node, context);
  }

  protected R visitQuerySpecification(QuerySpecification node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitSetOperation(SetOperation node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitUnion(Union node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitIntersect(Intersect node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitExcept(Except node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitTimestampLiteral(TimestampLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitWhenClause(WhenClause node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIntervalLiteral(IntervalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLambdaExpression(LambdaExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitBinaryLiteral(BinaryLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitInListExpression(InListExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitQualifiedNameReference(QualifiedNameReference node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDereferenceExpression(DereferenceExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNullIfExpression(NullIfExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNullLiteral(NullLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    process(node.getValue(), context);
    return visitExpression(node, context);
  }

  protected R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSelectItem(SelectItem node, C context) {
    return visitNode(node, context);
  }

  protected R visitSingleColumn(SingleColumn node, C context) {
    process(node.getExpression(), context);
    return visitSelectItem(node, context);
  }

  protected R visitAllColumns(AllColumns node, C context) {
    return visitSelectItem(node, context);
  }

  protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLikePredicate(LikePredicate node, C context) {
    process(node.getValue(), context);
    return visitExpression(node, context);
  }

  protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSubscriptExpression(SubscriptExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLongLiteral(LongLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return visitExpression(node, context);
  }

  protected R visitSubqueryExpression(SubqueryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSortItem(SortItem node, C context) {
    return visitNode(node, context);
  }

  protected R visitTable(Table node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitValues(Values node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitRow(Row node, C context) {
    return visitNode(node, context);
  }

  protected R visitTableSubquery(TableSubquery node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitAliasedRelation(AliasedRelation node, C context) {
    return visitRelation(node, context);
  }

  protected R visitSampledRelation(SampledRelation node, C context) {
    return visitRelation(node, context);
  }

  protected R visitJoin(Join node, C context) {
    return visitRelation(node, context);
  }

  protected R visitExists(ExistsPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  protected R visitFieldReference(FieldReference node, C context) {
    return visitExpression(node, context);
  }

  protected R visitWindow(Window node, C context) {
    return visitNode(node, context);
  }

  protected R visitWindowFrame(WindowFrame node, C context) {
    return visitNode(node, context);
  }

  protected R visitFrameBound(FrameBound node, C context) {
    return visitNode(node, context);
  }

  protected R visitTableElement(TableElement node, C context) {
    return visitNode(node, context);
  }

  protected R visitCreateStream(CreateStream node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateStreamAsSelect(CreateStreamAsSelect node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTable(CreateTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTableAsSelect(CreateTableAsSelect node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDropTable(DropTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitRenameTable(RenameTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitRenameColumn(RenameColumn node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateView(CreateView node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDropView(DropView node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDelete(Delete node, C context) {
    return visitStatement(node, context);
  }

  protected R visitGroupBy(GroupBy node, C context) {
    return visitNode(node, context);
  }

  protected R visitGroupingElement(GroupingElement node, C context) {
    return visitNode(node, context);
  }

  protected R visitGroupingSets(GroupingSets node, C context) {
    return visitGroupingElement(node, context);
  }

  protected R visitSimpleGroupBy(SimpleGroupBy node, C context) {
    return visitGroupingElement(node, context);
  }

  protected R visitSymbolReference(SymbolReference node, C context) {
    return visitExpression(node, context);
  }
}
