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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.StringToTimestampParser;
import java.time.ZoneId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class StatementRewriteForRowtimeTest {

  private static final StringToTimestampParser PARSER =
      new StringToTimestampParser("yyyy-MM-dd'T'HH:mm:ss.SSS");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private MetaStore metaStore;
  private StatementRewriteForRowtime rewritter;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    rewritter = new StatementRewriteForRowtime();
  }

  @Test
  public void shouldReplaceDatestring() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME > '2017-01-01T00:00:00.000';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(),
        equalTo(String.format("(ORDERS.ROWTIME > %d)", PARSER.parse("2017-01-01T00:00:00.000"))));
  }

  @Test
  public void shouldHandleInexactTimestamp() {
    // Given:
    final Query statement = buildSingleAst("SELECT * FROM orders where ROWTIME = '2017';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(),
        equalTo(String.format("(ORDERS.ROWTIME = %d)", PARSER.parse("2017-01-01T00:00:00.000"))));
  }

  @Test
  public void shouldHandleBetweenExpression() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME BETWEEN '2017-01-01' AND '2017-02-01';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(), equalTo(String.format(
        "(ORDERS.ROWTIME BETWEEN %d AND %d)",
        PARSER.parse("2017-01-01T00:00:00.000"),
        PARSER.parse("2017-02-01T00:00:00.000"))));
  }

  @Test
  public void shouldNotProcessStringsInFunctions() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME = foo('2017-01-01');");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME = FOO('2017-01-01'))"));
  }

  @Test
  public void shouldIgnoreNonRowtimeStrings() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME > '2017-01-01' AND ROWKEY = '2017-01-01';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(),
        equalTo(String.format("((ORDERS.ROWTIME > %d) AND (ORDERS.ROWKEY = '2017-01-01'))",
            PARSER.parse("2017-01-01T00:00:00.000"))));
  }

  @Test
  public void shouldHandleTimezones() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME = '2017-01-01T00:00:00.000+0100';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(), equalTo(String.format("(ORDERS.ROWTIME = %d)", PARSER
        .parse("2017-01-01T00:00:00.000", ZoneId.of("+0100")))));
  }

  @Test
  public void shouldNotProcessWhenRowtimeInFunction() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where foo(ROWTIME) = '2017-01-01';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(), containsString("(FOO(ORDERS.ROWTIME) = '2017-01-01')"));
  }

  @Test
  public void shouldNotProcessArithmetic() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where '2017-01-01' + 10000 > ROWTIME;");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(), containsString("(('2017-01-01' + 10000) > ORDERS.ROWTIME)"));
  }

  @Test
  public void shouldThrowParseError() {
    // Given:
    final Query statement = buildSingleAst("SELECT * FROM orders where ROWTIME = '2oo17-01-01';");
    final Expression predicate = statement.getWhere().get();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to parse timestamp '2oo17-01-01'");

    // When:
    rewritter.rewriteForRowtime(predicate);
  }

  @Test
  public void shouldThrowTimezoneParseError() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME = '2017-01-01T00:00:00.000+foo';");
    final Expression predicate = statement.getWhere().get();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to parse timestamp '2017-01-01T00:00:00.000+foo'");

    // When:
    rewritter.rewriteForRowtime(predicate);
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> T buildSingleAst(final String query) {
    return (T) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
  }
}