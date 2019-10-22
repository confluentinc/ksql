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

package io.confluent.ksql.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Test;

/**
 * This test class extends the tests in the SqlFormatterTest class that lives in the parser module
 * with tests that are specific to the engine. Notably ensuring the internal implementation details,
 * such as FETCH_FIELD_FROM_STRUCT aren't exposed to users.
 */
public class SqlFormatterEngineTest {

  private final MutableMetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));

  @Test
  public void shouldHideFetchFieldFromStructInternalUdf() {
    // Given:
    final Statement statement = parseSingle(
        "CREATE STREAM S AS SELECT o.address->city FROM ORDERS o;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is(
        "CREATE STREAM S AS SELECT O.ADDRESS->CITY \"ADDRESS__CITY\"\n"
            + "FROM ORDERS O\n"
            + "EMIT CHANGES"
    ));
  }

  @Test
  public void shouldFormatNoneInternalFunctionCalls() {
    // Given:
    final Statement statement = parseSingle(
        "CREATE STREAM s AS SELECT CEIL(o.orderid) FROM orders o;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is(
        "CREATE STREAM S AS SELECT CEIL(O.ORDERID) \"KSQL_COL_0\"\n"
            + "FROM ORDERS O\n"
            + "EMIT CHANGES"
    ));
  }

  private Statement parseSingle(final String sql) {
    return KsqlParserTestUtil.buildSingleAst(sql, metaStore).getStatement();
  }
}
