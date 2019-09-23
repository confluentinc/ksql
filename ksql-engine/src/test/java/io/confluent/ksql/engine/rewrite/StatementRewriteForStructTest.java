/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine.rewrite;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StatementRewriteForStructTest {

  private MetaStore metaStore;
  private StatementRewriteForStruct rewritter;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    rewritter = new StatementRewriteForStruct();
  }

  @Test
  public void shouldNotCreateFunctionCallIfNotNeeded() {
    // Given:
    final Query query = buildSingleStatement("SELECT orderid FROM orders;");

    // When:
    final Statement rewritten = rewritter.rewriteForStruct(query);

    // Then:
    final Expression col0 = getSelectExpression(0, rewritten);
    assertThat(col0, instanceOf(QualifiedNameReference.class));
    assertThat(col0.toString(), equalTo("ORDERS.ORDERID"));
  }

  @Test
  public void shouldRewriteQualifiedNameExpression() {
    // Given:
    final Query query = buildSingleStatement(
        "SELECT iteminfo->category->name FROM orders;");

    // When:
    final Statement rewritten = rewritter.rewriteForStruct(query);

    // Then:
    assertThat(
        getSelectFuncCall(0, rewritten).toString(),
        is("FETCH_FIELD_FROM_STRUCT(FETCH_FIELD_FROM_STRUCT(ORDERS.ITEMINFO, 'CATEGORY'), 'NAME')")
    );
  }

  @Test
  public void shouldRewriteSubscriptExpression() {
    // Given:
    final Query query = buildSingleStatement(
        "SELECT arraycol[0]->name as n0, mapcol['key']->name as n1 FROM nested_stream;");

    // When:
    final Statement rewritten = rewritter.rewriteForStruct(query);

    // Then:
    assertThat(
        getSelectFuncCall(0, rewritten).toString(),
        is("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ARRAYCOL[0], 'NAME')")
    );

    assertThat(
        getSelectFuncCall(1, rewritten).toString(),
        is("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.MAPCOL['key'], 'NAME')")
    );
  }

  @Test
  public void shouldRewriteSubscriptExpressionWithExpressionIndex() {
    // Given:
    final Query query = buildSingleStatement(
        "SELECT arraycol[CAST (item->id AS INTEGER)]->name as n0, mapcol['key']->name as n1 FROM nested_stream;");

    // When:
    final Statement rewritten = rewritter.rewriteForStruct(query);

    // Then:
    assertThat(
        getSelectFuncCall(0, rewritten).toString(),
        is("FETCH_FIELD_FROM_STRUCT("
            + "NESTED_STREAM.ARRAYCOL[CAST(FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ITEM, 'ID') AS INTEGER)], "
            + "'NAME'"
            + ")")
    );

    assertThat(
        getSelectFuncCall(1, rewritten).toString(),
        is("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.MAPCOL['key'], 'NAME')")
    );
  }

  @Test
  public void shouldRewriteExplainQuery() {
    // When:
    final Explain statement = buildSingleStatement("EXPLAIN SELECT address->state FROM orders;");

    // When:
    final Statement rewritten = rewritter.rewriteForStruct(statement);

    // Then:
    assertThat(rewritten, instanceOf(Explain.class));
    assertThat(
        ((Explain) rewritten).getStatement().toString(),
        containsString("FETCH_FIELD_FROM_STRUCT(ORDERS.ADDRESS, 'STATE')")
    );
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> T buildSingleStatement(final String simpleQuery) {
    return (T) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore)
        .getStatement();
  }

  private static Expression getSelectFuncCall(final int index, final Statement query) {
    final Expression expression = getSelectExpression(index, query);
    assertThat(expression, is(instanceOf(FunctionCall.class)));
    return expression;
  }

  private static Expression getSelectExpression(final int index, final Statement query) {
    assertThat(query, is(instanceOf(Query.class)));

    final List<SelectItem> selects = ((Query) query).getSelect().getSelectItems();
    assertThat(selects, hasSize(greaterThan(index)));

    return ((SingleColumn) selects.get(index)).getExpression();
  }
}