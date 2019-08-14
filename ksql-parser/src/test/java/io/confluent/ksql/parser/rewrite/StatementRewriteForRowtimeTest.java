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

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class StatementRewriteForRowtimeTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
  }

  @Test
  public void shouldWrapDatestring() {
    final String query = "SELECT * FROM orders where ROWTIME > '2017-01-01T00:00:00.000';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME > 1483257600000)"));
  }

  @Test
  public void shouldHandleInexactTimestamp() {
    final String query = "SELECT * FROM orders where ROWTIME = '2017';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME = 1483257600000)"));
  }

  @Test
  public void shouldHandleBetweenExpression() {
    final String query = "SELECT * FROM orders where ROWTIME BETWEEN '2017-01-01' AND '2017-02-01';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME BETWEEN 1483257600000 AND 1485936000000)"));
  }

  @Test
  public void shouldNotProcessStringsInFunctions() {
    final String query = "SELECT * FROM orders where ROWTIME = foo('2017-01-01');";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME = FOO('2017-01-01'))"));
  }

  @Test
  public void shouldIgnoreNonRowtimeStrings() {
    final String query = "SELECT * FROM orders where ROWTIME > '2017-01-01' AND ROWKEY = '2017-01-01';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("((ORDERS.ROWTIME > 1483257600000) AND (ORDERS.ROWKEY = '2017-01-01'))"));
  }

  @Test
  public void shouldHandleTimezones() {
    final String simpleQuery = "SELECT * FROM orders where ROWTIME = '2017-01-01T00:00:00.000+0100';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), containsString("(ORDERS.ROWTIME = 1483225200000)"));
  }

  @Test
  public void shouldNotProcessWhenRowtimeInFunction() {
    final String simpleQuery = "SELECT * FROM orders where foo(ROWTIME) = '2017-01-01';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), containsString("(FOO(ORDERS.ROWTIME) = '2017-01-01')"));
  }

  @Test
  public void shouldNotProcessArithmetic() {
    final String simpleQuery = "SELECT * FROM orders where '2017-01-01' + 10000 > ROWTIME;";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), containsString("(('2017-01-01' + 10000) > ORDERS.ROWTIME)"));
  }

  @Test
  public void shouldThrowParseError() {
    // Given:
    final String simpleQuery = "SELECT * FROM orders where ROWTIME = '2oo17-01-01';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to parse timestamp '2oo17-01-01'");

    // When:
    new StatementRewriteForRowtime(predicate).rewriteForRowtime();
  }

  @Test
  public void shouldThrowTimezoneParseError() {
    final String simpleQuery = "SELECT * FROM orders where ROWTIME = '2017-01-01T00:00:00.000+foo';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to parse timestamp '2017-01-01T00:00:00.000+foo'");

    // When:
    new StatementRewriteForRowtime(predicate).rewriteForRowtime();
  }
}