/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import io.confluent.ksql.parser.AssertTable;
import io.confluent.ksql.parser.DropType;
import javax.annotation.Nullable;

public abstract class AstVisitor<R, C> {
  private final R defaultResult;

  protected AstVisitor() {
    this(null);
  }

  protected AstVisitor(final R defaultResult) {
    this.defaultResult = defaultResult;
  }

  public R process(final AstNode node, @Nullable final C context) {
    return node.accept(this, context);
  }

  protected R visitNode(final AstNode node, final C context) {
    return defaultResult;
  }

  protected R visitStatements(final Statements node, final C context) {
    return visitNode(node, context);
  }

  protected R visitStatement(final Statement node, final C context) {
    return visitNode(node, context);
  }

  protected R visitQuery(final Query node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitExplain(final Explain node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitShowColumns(final ShowColumns node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitShowFunctions(final ListFunctions node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitSelect(final Select node, final C context) {
    return visitNode(node, context);
  }

  protected R visitRelation(final Relation node, final C context) {
    return visitNode(node, context);
  }

  protected R visitSelectItem(final SelectItem node, final C context) {
    return visitNode(node, context);
  }

  protected R visitSingleColumn(final SingleColumn node, final C context) {
    return visitSelectItem(node, context);
  }

  protected R visitAllColumns(final AllColumns node, final C context) {
    return visitSelectItem(node, context);
  }

  protected R visitStructAll(final StructAll node, final C context) {
    return visitSelectItem(node, context);
  }

  protected R visitTable(final Table node, final C context) {
    return visitRelation(node, context);
  }

  protected R visitAliasedRelation(final AliasedRelation node, final C context) {
    return visitRelation(node, context);
  }

  protected R visitJoin(final Join node, final C context) {
    return visitRelation(node, context);
  }

  protected R visitJoinedSource(final JoinedSource joinedSource, final C context) {
    return visitRelation(joinedSource, context);
  }

  protected R visitWithinExpression(final WithinExpression node, final C context) {
    return visitNode(node, context);
  }

  protected R visitWindowExpression(final WindowExpression node, final C context) {
    return visitNode(node, context);
  }

  protected R visitTableElement(final TableElement node, final C context) {
    return visitNode(node, context);
  }

  protected R visitCreateStream(final CreateStream node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateStreamAsSelect(final CreateStreamAsSelect node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTable(final CreateTable node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTableAsSelect(final CreateTableAsSelect node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitInsertInto(final InsertInto node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitInsertValues(final InsertValues node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitDropStream(final DropStream node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitDropTable(final DropTable node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitPartitionBy(final PartitionBy node, final C context) {
    return visitNode(node, context);
  }

  protected R visitGroupBy(final GroupBy node, final C context) {
    return visitNode(node, context);
  }

  protected R visitPauseQuery(final PauseQuery node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitResumeQuery(final ResumeQuery node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitTerminateQuery(final TerminateQuery node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitListStreams(final ListStreams node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitListTables(final ListTables node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitListTypes(final ListTypes listTypes, final C context) {
    return visitStatement(listTypes, context);
  }

  protected R visitListConnectorPlugins(final ListConnectorPlugins node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitUnsetProperty(final UnsetProperty node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitSetProperty(final SetProperty node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitAlterSystemProperty(final AlterSystemProperty node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitDefineVariable(final DefineVariable node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitUndefineVariable(final UndefineVariable node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitListVariables(final ListVariables node, final C context) {
    return visitStatement(node, context);
  }

  public R visitRegisterType(final RegisterType node, final C context) {
    return visitStatement(node, context);
  }

  public R visitDropType(final DropType node, final C context) {
    return visitStatement(node, context);
  }

  public R visitAssertValues(final AssertValues node, final C context) {
    return visitStatement(node.getStatement(), context);
  }

  public R visitAssertStream(final AssertStream node, final C context) {
    return visitStatement(node.getStatement(), context);
  }

  public R visitAssertTable(final AssertTable node, final C context) {
    return visitStatement(node.getStatement(), context);
  }

  public R visitAssertTombstone(final AssertTombstone node, final C context) {
    return visitStatement(node.getStatement(), context);
  }

  public R visitAlterOption(final AlterOption node, final C context) {
    return visitNode(node, context);
  }

  public R visitAlterSource(final AlterSource node, final C context) {
    return visitNode(node, context);
  }

  protected R visitDescribeStreams(final DescribeStreams node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitDescribeTables(final DescribeTables node, final C context) {
    return visitStatement(node, context);
  }
}
