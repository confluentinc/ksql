/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.rewrite;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.MetaStoreFixture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class StatementRewriteForStructTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }

  @Test
  public void shouldCreateCorrectFunctionCallExpression() {
    final String simpleQuery = "SELECT iteminfo->category->name, address->state FROM orders;";
    final Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    final QuerySpecification querySpecification = getQuerySpecification(statement);
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    final Expression col0 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(0))
        .getExpression();
    final Expression col1 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(1))
        .getExpression();

    assertThat(col0, instanceOf(FunctionCall.class));
    assertThat(col1, instanceOf(FunctionCall.class));

    assertThat(col0.toString(), equalTo(
        "FETCH_FIELD_FROM_STRUCT(FETCH_FIELD_FROM_STRUCT(ORDERS.ITEMINFO, 'CATEGORY'), 'NAME')"));
    assertThat(col1.toString(), equalTo("FETCH_FIELD_FROM_STRUCT(ORDERS.ADDRESS, 'STATE')"));

  }

  @Test
  public void shouldNotCreateFunctionCallIfNotNeeded() {
    final String simpleQuery = "SELECT orderid FROM orders;";
    final Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    final QuerySpecification querySpecification = getQuerySpecification(statement);
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(1));
    final Expression col0 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(0))
        .getExpression();

    assertThat(col0, instanceOf(DereferenceExpression.class));
    assertThat(col0.toString(), equalTo("ORDERS.ORDERID"));
  }


  @Test
  public void shouldCreateCorrectFunctionCallExpressionWithSubscript() {
    final String simpleQuery = "SELECT arraycol[0]->name as n0, mapcol['key']->name as n1 FROM nested_stream;";
    final Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    final QuerySpecification querySpecification = getQuerySpecification(statement);
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    final Expression col0 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(0))
        .getExpression();
    final Expression col1 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(1))
        .getExpression();

    assertThat(col0, instanceOf(FunctionCall.class));
    assertThat(col1, instanceOf(FunctionCall.class));

    assertThat(col0.toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ARRAYCOL[0], 'NAME')"));
    assertThat(col1.toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.MAPCOL['key'], 'NAME')"));
  }


  @Test
  public void shouldCreateCorrectFunctionCallExpressionWithSubscriptWithExpressionIndex() {
    final String simpleQuery = "SELECT arraycol[CAST (item->id AS INTEGER)]->name as n0, mapcol['key']->name as n1 FROM nested_stream;";
    final Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    final QuerySpecification querySpecification = getQuerySpecification(statement);
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    final Expression col0 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(0))
        .getExpression();
    final Expression col1 = ((SingleColumn) querySpecification.getSelect().getSelectItems().get(1))
        .getExpression();

    assertThat(col0, instanceOf(FunctionCall.class));
    assertThat(col1, instanceOf(FunctionCall.class));

    assertThat(col0.toString(), equalTo(
        "FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ARRAYCOL[CAST(FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ITEM, 'ID') AS INTEGER)], 'NAME')"));
    assertThat(col1.toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.MAPCOL['key'], 'NAME')"));
  }

  private QuerySpecification getQuerySpecification(Statement statement) {
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    return (QuerySpecification) query.getQueryBody();
  }


  @Test
  public void shouldEnsureRewriteRequirementCorrectly() {
    assertThat("Query should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(Query.class)));
    assertThat("CSAS should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(CreateStreamAsSelect.class)));
    assertThat("CTAS should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(CreateTableAsSelect.class)));
    assertThat("Insert Into should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(InsertInto.class)));
  }

  @Test
  public void shouldFailTestIfStatementShouldBeRewritten() {
    assertThat("Incorrect rewrite requirement enforcement.", !StatementRewriteForStruct.requiresRewrite(EasyMock.mock(CreateTable.class)));
  }

}