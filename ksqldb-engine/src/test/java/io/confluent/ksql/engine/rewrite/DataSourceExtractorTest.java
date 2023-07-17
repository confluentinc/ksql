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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceExtractorTest {

  private static final SourceName TEST1 = SourceName.of("TEST1");
  private static final SourceName TEST2 = SourceName.of("TEST2");

  private static final SourceName T1 = SourceName.of("T1");
  private static final SourceName T2 = SourceName.of("T2");

  private static final MetaStore META_STORE = MetaStoreFixture
      .getNewMetaStore(mock(FunctionRegistry.class));
  private static final ColumnName COL0 = ColumnName.of("COL0");

  private DataSourceExtractor extractor;

  @Before
  public void setUp() {
    extractor = new DataSourceExtractor(META_STORE);
  }

  @Test
  public void shouldExtractUnaliasedDataSources() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertContainsAlias(TEST1);
  }

  @Test
  public void shouldHandleAliasedDataSources() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1 t;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertContainsAlias(SourceName.of("T"));

  }

  @Test
  public void shouldThrowIfSourceDoesNotExist() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM UNKNOWN;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> extractor.extractDataSources(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist."));
  }

  @Test
  public void shouldExtractUnaliasedJoinDataSources() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1 JOIN TEST2"
        + " ON test1.col1 = test2.col1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertContainsAlias(TEST1, TEST2);
  }

  @Test
  public void shouldHandleAliasedJoinDataSources() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1 t1 JOIN TEST2 t2"
        + " ON test1.col1 = test2.col1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertContainsAlias(T1, T2);
  }

  @Test
  public void shouldThrowIfLeftJoinSourceDoesNotExist() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM UNKNOWN JOIN TEST2"
        + " ON UNKNOWN.col1 = test2.col1;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> extractor.extractDataSources(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist."));
  }

  @Test
  public void shouldThrowIfInnerJoinSourceDoesNotExist() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1 JOIN UNKNOWN"
        + " ON test1.col1 = UNKNOWN.col1;");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> extractor.extractDataSources(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UNKNOWN does not exist."));
  }

  @Test
  public void shouldDetectClashingColumnNames() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1 t1 JOIN TEST2 t2"
        + " ON test1.col1 = test2.col1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertThat("should clash", extractor.isClashingColumnName(COL0));
  }

  @Test
  public void shouldDetectClashingColumnNamesEvenIfOneJoinSourceHasNoClash() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM ORDERS "
        + "JOIN TEST1 t1 ON ORDERS.ITEMID = T1.COL1 "
        + "JOIN TEST2 t2 ON test1.col1 = test2.col1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertThat("should clash", extractor.isClashingColumnName(COL0));
  }

  @Test
  public void shouldDetectNoneClashingColumnNames() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM ORDERS "
        + "JOIN TEST1 t1 ON ORDERS.ITEMID = T1.COL1 "
        + "JOIN TEST2 t2 ON test1.col1 = test2.col1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    assertThat("should not clash", !extractor.isClashingColumnName(ColumnName.of("ORDERTIME")));
  }

  @Test
  public void shouldIncludePseudoColumnsInClashingNames() {
    // Given:
    final AstNode stmt = givenQuery("SELECT * FROM TEST1 t1 JOIN TEST2 t2"
        + " ON test1.col1 = test2.col1;");

    // When:
    extractor.extractDataSources(stmt);

    // Then:
    SystemColumns.pseudoColumnNames().forEach(pseudoCol ->
        assertThat(pseudoCol + " should clash", extractor.isClashingColumnName(pseudoCol))
    );
  }

  private void assertContainsAlias(final SourceName... alias) {
    assertThat(
        extractor.getAllSources()
            .stream()
            .map(AliasedDataSource::getAlias)
            .collect(Collectors.toList()),
        containsInAnyOrder(alias));
  }

  private static AstNode givenQuery(final String sql) {
    final List<ParsedStatement> statements = new DefaultKsqlParser().parse(sql);
    assertThat(statements, hasSize(1));
    return new AstBuilder(META_STORE).buildStatement(statements.get(0).getStatement());
  }
}