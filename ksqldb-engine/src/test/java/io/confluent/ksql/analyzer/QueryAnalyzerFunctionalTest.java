/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.analyzer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import java.io.File;
import java.util.Optional;
import org.junit.Test;

/**
 * DO NOT ADD NEW TESTS TO THIS FILE
 *
 * <p>Instead add new JSON based tests to QueryTranslationTest
 *
 * <p>This test file is more of a functional test, which is better implemented using QTT.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
public class QueryAnalyzerFunctionalTest {

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
  private final QueryAnalyzer queryAnalyzer =
      new QueryAnalyzer(metaStore, "prefix-~");

  @Test
  public void shouldAnalyseTableFunctions() {

    // We need to load udfs for this
    final UserFunctionLoader loader = new UserFunctionLoader(functionRegistry, new File(""),
        Thread.currentThread().getContextClassLoader(),
        s -> false,
        Optional.empty(), true
    );
    loader.load();

    // Given:
    final Query query = givenQuery("SELECT ID, EXPLODE(ARR1) FROM SENSOR_READINGS;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getTableFunctions(), hasSize(1));
    assertThat(analysis.getTableFunctions().get(0).getName().text(), equalTo("EXPLODE"));
  }

  @Test
  public void shouldThrowIfUdafsAndNoGroupBy() {
    // Given:
    final Query query = givenQuery("select itemid, sum(orderunits) from orders EMIT CHANGES;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> queryAnalyzer.analyze(query, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Use of aggregate function SUM requires a GROUP BY clause."
    ));
  }

  @Test
  public void shouldHandleValueFormat() {
    // Given:
    final PreparedStatement<CreateStreamAsSelect> statement = KsqlParserTestUtil.buildSingleAst(
        "create stream s with(value_format='delimited') as select * from test1;", metaStore);
    final Query query = statement.getStatement().getQuery();
    final Optional<Sink> sink = Optional.of(statement.getStatement().getSink());

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, sink);

    // Then:
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat().getFormat(),
        is(FormatFactory.DELIMITED));
  }

  private Query givenQuery(final String sql) {
    return KsqlParserTestUtil.<Query>buildSingleAst(sql, metaStore).getStatement();
  }
}