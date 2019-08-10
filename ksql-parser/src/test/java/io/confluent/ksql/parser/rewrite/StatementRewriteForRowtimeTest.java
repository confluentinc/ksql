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

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class StatementRewriteForRowtimeTest {
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

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME > STRINGTOTIMESTAMP('2017-01-01T00:00:00.000', 'yyyy-MM-dd''T''HH:mm:ss.SSS'))"));
  }

  @Test
  public void shouldHandleInexactTimestamp() {
    final String query = "SELECT * FROM orders where ROWTIME = '2017';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME = STRINGTOTIMESTAMP('2017-01-01T00:00:00.000', 'yyyy-MM-dd''T''HH:mm:ss.SSS'))"));
  }

  @Test
  public void shouldHandleBetweenExpression() {
    final String query = "SELECT * FROM orders where ROWTIME BETWEEN '2017-01-01' AND '2017-02-01';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(query, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), equalTo("(ORDERS.ROWTIME BETWEEN"
        + " STRINGTOTIMESTAMP('2017-01-01T00:00:00.000', 'yyyy-MM-dd''T''HH:mm:ss.SSS') AND"
        + " STRINGTOTIMESTAMP('2017-02-01T00:00:00.000', 'yyyy-MM-dd''T''HH:mm:ss.SSS'))"));
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

    assertThat(rewritten.toString(), equalTo("((ORDERS.ROWTIME > STRINGTOTIMESTAMP('2017-01-01T00:00:00.000', 'yyyy-MM-dd''T''HH:mm:ss.SSS')) AND (ORDERS.ROWKEY = '2017-01-01'))"));
  }

  @Test
  public void shouldHandleTimezones() {
    final String simpleQuery = "SELECT * FROM orders where ROWTIME = '2017-01-01T00:00:00.000+0100';";
    final Query statement = (Query) KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    final Expression predicate = statement.getWhere().get();
    final Expression rewritten = new StatementRewriteForRowtime(predicate).rewriteForRowtime();

    assertThat(rewritten.toString(), containsString("(ORDERS.ROWTIME = STRINGTOTIMESTAMP('2017-01-01T00:00:00.000', 'yyyy-MM-dd''T''HH:mm:ss.SSS', '+0100'))"));
  }
}