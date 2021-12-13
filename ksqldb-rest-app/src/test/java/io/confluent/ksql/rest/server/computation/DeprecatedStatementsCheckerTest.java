/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static io.confluent.ksql.rest.server.computation.DeprecatedStatementsChecker.Deprecations.DEPRECATED_STREAM_STREAM_JOIN_WITH_NO_GRACE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class DeprecatedStatementsCheckerTest {
  private final KsqlParser STATEMENT_PARSER = new DefaultKsqlParser();

  private final SourceName STREAM_1 = SourceName.of("STREAM_1");
  private final SourceName STREAM_2 = SourceName.of("STREAM_2");
  private final SourceName STREAM_3 = SourceName.of("STREAM_3");
  private final SourceName TABLE_1 = SourceName.of("TABLE_1");

  private DeprecatedStatementsChecker statementsChecker;

  @Mock
  private MetaStore metaStore;

  @Before
  public void setup() {
    statementsChecker = new DeprecatedStatementsChecker(metaStore);

    final DataSource streamSource1 = newKsqlStream(STREAM_1);
    when(metaStore.getSource(STREAM_1)).thenReturn(streamSource1);

    final DataSource streamSource2 = newKsqlStream(STREAM_2);
    when(metaStore.getSource(STREAM_2)).thenReturn(streamSource2);

    final DataSource streamSource3 = newKsqlStream(STREAM_3);
    when(metaStore.getSource(STREAM_3)).thenReturn(streamSource3);

    final DataSource tableSource1 = newKsqlTable(TABLE_1);
    when(metaStore.getSource(TABLE_1)).thenReturn(tableSource1);
  }

  @Test
  public void shouldDeprecateStreamStreamJoinsWithNoGrace() {
    for (final JoinType joinType : JoinType.values()) {
      checkDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS r WITHIN 1 SECOND ON l.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text()
      ));
    }
  }

  @Test
  public void shouldDeprecateStreamStreamJoinsWithNoGraceOnMultiStreamStreamJoinOrder1() {
    for (final JoinType joinType : JoinType.values()) {
      checkDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS m WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON l.K = m.K "
              + "%s JOIN %s AS r WITHIN 1 SECOND ON m.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_3.text()
      ));
    }
  }

  @Test
  public void shouldDeprecateStreamStreamJoinsWithNoGraceOnMultiStreamStreamJoinOrder2() {
    for (final JoinType joinType : JoinType.values()) {
      checkDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS r WITHIN 1 SECOND ON m.K = r.K "
              + "%s JOIN %s AS m WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON l.K = m.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_3.text()
      ));
    }
  }

  @Test
  public void shouldDeprecateStreamStreamJoinsWithNoGraceOnMultiStreamStreamTableJoinOrder1() {
    for (final JoinType joinType : JoinType.values()) {
      checkDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS m WITHIN 1 SECOND ON l.K = m.K "
              + "%s JOIN %s AS r ON l.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          TABLE_1.text()
      ));
    }
  }

  @Test
  public void shouldDeprecateStreamStreamJoinsWithNoGraceOnMultiStreamStreamTableJoinOrder2() {
    for (final JoinType joinType : JoinType.values()) {
      checkDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS r ON l.K = r.K "
              + "%s JOIN %s AS m WITHIN 1 SECOND ON l.K = m.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          TABLE_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text()
      ));
    }
  }

  @Test
  public void shouldNotDeprecateStreamStreamJoinsWithGrace() {
    for (final JoinType joinType : JoinType.values()) {
      checkNoDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS r WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON l.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text()
      ));
    }
  }

  @Test
  public void shouldNotDeprecateStreamStreamJoinWithGraceOnMultiStreamStreamJoin() {
    for (final JoinType joinType : JoinType.values()) {
      checkNoDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS m WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON l.K = m.K "
              + "%s JOIN %s AS r WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON m.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_3.text()
      ));
    }
  }

  @Test
  public void shouldNotDeprecateStreamStreamJoinsWithGraceOnMultiStreamStreamTableJoinOrder1() {
    for (final JoinType joinType : JoinType.values()) {
      checkNoDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS m WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON l.K = m.K "
              + "%s JOIN %s AS r ON l.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          TABLE_1.text()
      ));
    }
  }

  @Test
  public void shouldNotDeprecateStreamStreamJoinsWithGraceOnMultiStreamStreamTableJoinOrder2() {
    for (final JoinType joinType : JoinType.values()) {
      checkNoDeprecatedStatement(String.format(
          "SELECT * FROM %s AS l "
              + "%s JOIN %s AS r ON l.K = r.K "
              + "%s JOIN %s AS m WITHIN 1 SECOND GRACE PERIOD 1 SECOND ON l.K = m.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          TABLE_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text()
      ));
    }
  }

  @Test
  public void shouldDeprecateStreamStreamJoinInsideCreateAs() {
    for (final JoinType joinType : JoinType.values()) {
      checkDeprecatedStatement(String.format(
          "CREATE STREAM DEPRECATED_QUERY AS "
              + "SELECT * FROM %s AS l "
              + "%s JOIN %s AS r WITHIN 1 SECOND ON l.K = r.K;",
          STREAM_1.text(),
          (joinType == JoinType.OUTER) ? "FULL OUTER" : joinType,
          STREAM_2.text()
      ));
    }
  }

  private void checkDeprecatedStatement(final String statementText) {
    // Given
    final Statement statement = parse(statementText);

    // When
    final Optional<DeprecatedStatementsChecker.Deprecations> deprecations =
        statementsChecker.checkStatement(statement);

    // Then
    assertThat(deprecations, is(Optional.of(DEPRECATED_STREAM_STREAM_JOIN_WITH_NO_GRACE)));
  }

  private void checkNoDeprecatedStatement(final String statementText) {
    // Given
    final Statement statement = parse(statementText);

    // When
    final Optional<DeprecatedStatementsChecker.Deprecations> deprecations =
        statementsChecker.checkStatement(statement);

    // Then
    assertThat(deprecations, is(Optional.empty()));
  }

  private Statement parse(final String statementText) {
    final TypeRegistry mockTypeRegistry = Mockito.mock(TypeRegistry.class);
    final AstBuilder statementBuilder = new AstBuilder(mockTypeRegistry);

    return statementBuilder.buildStatement(
        Iterables.getOnlyElement(STATEMENT_PARSER.parse(statementText)).getStatement()
    );
  }

  private DataSource newKsqlStream(final SourceName sourceName) {
    return new KsqlStream<>(
        "query",
        sourceName,
        mock(LogicalSchema.class),
        Optional.empty(),
        false,
        mock(KsqlTopic.class),
        false
    );
  }

  private DataSource newKsqlTable(final SourceName sourceName) {
    return new KsqlTable<>(
        "query",
        sourceName,
        mock(LogicalSchema.class),
        Optional.empty(),
        false,
        mock(KsqlTopic.class),
        false
    );
  }
}
