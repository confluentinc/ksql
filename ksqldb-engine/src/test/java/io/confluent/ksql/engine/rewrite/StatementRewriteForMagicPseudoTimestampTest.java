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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class StatementRewriteForMagicPseudoTimestampTest {

  private static final long A_TIMESTAMP = 1234567890L;
  private static final long ANOTHER_TIMESTAMP = 1234568890L;

  @Mock
  private PartialStringToTimestampParser parser;
  private MetaStore metaStore;
  private StatementRewriteForMagicPseudoTimestamp rewriter;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    rewriter = new StatementRewriteForMagicPseudoTimestamp(parser);

    when(parser.parse(any()))
        .thenReturn(A_TIMESTAMP)
        .thenReturn(ANOTHER_TIMESTAMP);
  }

  @Test
  public void shouldPassRowTimeStringsToTheParser() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME = '2017-01-01T00:44:00.000';");

    // When:
    rewriter.rewrite(predicate);

    // Then:
    verify(parser).parse("2017-01-01T00:44:00.000");
  }

  @Test
  public void shouldNotReplaceComparisonRowTimeAndNonString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME > 10.25;");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten, is(predicate));
  }

  @Test
  public void shouldReplaceComparisonOfRowTimeAndString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME > '2017-01-01T00:00:00.000';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten.toString(), is(String.format("(ORDERS.ROWTIME > %d)", A_TIMESTAMP)));
  }

  @Test
  public void shouldReplaceComparisonOfWindowStartAndString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where WINDOWSTART > '2017-01-01T00:00:00.000';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten.toString(), is(String.format("(WINDOWSTART > %d)", A_TIMESTAMP)));
  }

  @Test
  public void shouldReplaceComparisonOfWindowEndAndString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where WINDOWEND > '2017-01-01T00:00:00.000';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten.toString(), is(String.format("(WINDOWEND > %d)", A_TIMESTAMP)));
  }

  @Test
  public void shouldReplaceComparisonInReverseOrder() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where '2017-01-01T00:00:00.000' < ROWTIME;");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten.toString(), is(String.format("(%d < ORDERS.ROWTIME)", A_TIMESTAMP)));
  }

  @Test
  public void shouldReplaceBetweenRowTimeAndStrings() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME BETWEEN '2017-01-01' AND '2017-02-01';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(
        rewritten.toString(),
        is(String.format("(ORDERS.ROWTIME BETWEEN %d AND %d)", A_TIMESTAMP, ANOTHER_TIMESTAMP))
    );
  }

  @Test
  public void shouldReplaceBetweenWindowStartAndStrings() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where WINDOWSTART BETWEEN '2017-01-01' AND '2017-02-01';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(
        rewritten.toString(),
        is(String.format("(WINDOWSTART BETWEEN %d AND %d)", A_TIMESTAMP, ANOTHER_TIMESTAMP))
    );
  }

  @Test
  public void shouldReplaceBetweenWindowEndAndStrings() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where WINDOWEND BETWEEN '2017-01-01' AND '2017-02-01';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(
        rewritten.toString(),
        is(String.format("(WINDOWEND BETWEEN %d AND %d)", A_TIMESTAMP, ANOTHER_TIMESTAMP))
    );
  }

  @Test
  public void shouldReplaceBetweenOnMinString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where WINDOWSTART BETWEEN '2017-01-01' AND 1236987;");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(
        rewritten.toString(),
        is(String.format("(WINDOWSTART BETWEEN %d AND 1236987)", A_TIMESTAMP))
    );
  }

  @Test
  public void shouldReplaceBetweenOnMaxString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where WINDOWEND BETWEEN 1236987 AND '2017-01-01';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(
        rewritten.toString(),
        is(String.format("(WINDOWEND BETWEEN 1236987 AND %d)", A_TIMESTAMP))
    );
  }

  @Test
  public void shouldNotReplaceBetweenExpressionOnNonString() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME BETWEEN 123456 AND 147258;");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten, is(predicate));
  }

  @Test
  public void shouldReplaceQualifiedColumns() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ORDERS.ROWTIME > '2017-01-01T00:00:00.000';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten.toString(), is(String.format("(ORDERS.ROWTIME > %d)", A_TIMESTAMP)));
  }

  @Test
  public void shouldNotReplaceStringsInFunctions() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME = foo('2017-01-01');");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    verify(parser, never()).parse(any());
    assertThat(rewritten.toString(), is("(ORDERS.ROWTIME = FOO('2017-01-01'))"));
  }

  @Test
  public void shouldNotReplaceUnsupportedColumns() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where ROWTIME > '2017-01-01' AND ORDERTIME = '2017-01-01';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    assertThat(rewritten.toString(),
        is(String.format("((ORDERS.ROWTIME > %d) AND (ORDERS.ORDERTIME = '2017-01-01'))",
            A_TIMESTAMP)));
  }

  @Test
  public void shouldNotReplaceSupportedInFunction() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where foo(ROWTIME) = '2017-01-01';");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    verify(parser, never()).parse(any());
    assertThat(rewritten.toString(), containsString("(FOO(ORDERS.ROWTIME) = '2017-01-01')"));
  }

  @Test
  public void shouldNotReplaceArithmetic() {
    // Given:
    final Expression predicate = getPredicate(
        "SELECT * FROM orders where '2017-01-01' + 10000 > ROWTIME;");

    // When:
    final Expression rewritten = rewriter.rewrite(predicate);

    // Then:
    verify(parser, never()).parse(any());
    assertThat(rewritten.toString(), containsString("(('2017-01-01' + 10000) > ORDERS.ROWTIME)"));
  }

  @Test
  public void shouldThrowParseError() {
    // Given:
    final Expression predicate = getPredicate("SELECT * FROM orders where ROWTIME = '2017-01-01';");
    when(parser.parse(any())).thenThrow(new IllegalArgumentException("it no good"));

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> rewriter.rewrite(predicate)
    );

    // Then:
    assertThat(e.getMessage(), containsString("it no good"));
  }

  private Expression getPredicate(final String querySql) {
    final Query statement = (Query) KsqlParserTestUtil
        .buildSingleAst(querySql, metaStore)
        .getStatement();

    return statement.getWhere().get();
  }
}