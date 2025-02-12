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

import static io.confluent.ksql.function.UserFunctionLoaderTestUtil.loadAllUserFunctions;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
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

  private static final boolean PULL_LIMIT_CLAUSE_ENABLED = true;

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
  private final QueryAnalyzer queryAnalyzer =
      new QueryAnalyzer(metaStore, "prefix-~", PULL_LIMIT_CLAUSE_ENABLED);

  @Test
  public void shouldAnalyseTableFunctions() {
    loadAllUserFunctions(functionRegistry);

    // Given:
    final Query query = givenQuery("SELECT ID, EXPLODE(ARR1) FROM SENSOR_READINGS EMIT CHANGES;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getTableFunctions(), hasSize(1));
    assertThat(analysis.getTableFunctions().get(0).getName().text(), equalTo("EXPLODE"));
  }

  @Test
  public void shouldAnalyseAggregateFunctions() {
    loadAllUserFunctions(functionRegistry);

    // Given:
    final Query query = givenQuery("SELECT ID, COUNT(ARR1) FROM SENSOR_READINGS EMIT CHANGES;");

    // When:
    final Analysis analysis = queryAnalyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getAggregateFunctions(), hasSize(1));
    assertThat(analysis.getAggregateFunctions().get(0).getName().text(), equalTo("COUNT"));
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
    assertThat(analysis.getInto().get().getNewTopic().get().getValueFormat().getFormat(),
        is(FormatFactory.DELIMITED.name()));
  }

  private Query givenQuery(final String sql) {
    return KsqlParserTestUtil.<Query>buildSingleAst(sql, metaStore).getStatement();
  }
}