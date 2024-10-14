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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AnalysisTest {
  
  private static final boolean ROWPARTITION_ROWOFFSET_ENABLED = true;

  private static final SourceName ALIAS = SourceName.of("ds1");
  private static final FormatInfo A_FORMAT = FormatInfo.of("JSON");
  private static final WindowInfo A_WINDOW = WindowInfo.of(WindowType.SESSION, Optional.empty());

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("bob"), SqlTypes.BIGINT)
      .build();

  @Mock
  private RefinementInfo refinementInfo;
  @Mock
  private KsqlStream<?> dataSource;
  @Mock
  private BiFunction<Map<SourceName, LogicalSchema>, Boolean, SourceSchemas> sourceSchemasFactory;
  @Mock
  private WindowExpression windowExpression;

  private Analysis analysis;

  @Before
  public void setUp() {
    analysis = new Analysis(Optional.of(refinementInfo), sourceSchemasFactory, ROWPARTITION_ROWOFFSET_ENABLED, true);

    when(dataSource.getSchema()).thenReturn(SOURCE_SCHEMA);
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldGetNoneWindowedSourceSchemasPreAggregate() {
    // Given:
    analysis.addDataSource(ALIAS, dataSource);

    givenNoneWindowedSource(dataSource);

    // When:
    analysis.getFromSourceSchemas(false);

    // Then:
    verify(sourceSchemasFactory).apply(
        ImmutableMap.of(
            ALIAS,
            SOURCE_SCHEMA.withPseudoAndKeyColsInValue(false, ROWPARTITION_ROWOFFSET_ENABLED)
        ),
        ROWPARTITION_ROWOFFSET_ENABLED
    );
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldGetWindowedSourceSchemasPreAggregate() {
    // Given:
    analysis.addDataSource(ALIAS, dataSource);

    givenWindowedSource(dataSource);

    // When:
    analysis.getFromSourceSchemas(false);

    // Then:
    verify(sourceSchemasFactory).apply(
        ImmutableMap.of(
            ALIAS,
            SOURCE_SCHEMA.withPseudoAndKeyColsInValue(true, ROWPARTITION_ROWOFFSET_ENABLED)
        ),
        ROWPARTITION_ROWOFFSET_ENABLED
    );
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldGetWindowedGroupBySourceSchemasPreAggregate() {
    // Given:
    analysis.addDataSource(ALIAS, dataSource);

    givenNoneWindowedSource(dataSource);
    analysis.setWindowExpression(windowExpression);

    // When:
    analysis.getFromSourceSchemas(false);

    // Then:
    verify(sourceSchemasFactory).apply(
        ImmutableMap.of(
            ALIAS,
            SOURCE_SCHEMA.withPseudoAndKeyColsInValue(false, ROWPARTITION_ROWOFFSET_ENABLED)
        ),
        ROWPARTITION_ROWOFFSET_ENABLED
    );
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldGetNonWindowedSourceSchemasPostAggregate() {
    // Given:
    analysis.addDataSource(ALIAS, dataSource);

    givenNoneWindowedSource(dataSource);

    // When:
    analysis.getFromSourceSchemas(true);

    // Then:
    verify(sourceSchemasFactory).apply(
        ImmutableMap.of(
            ALIAS,
            SOURCE_SCHEMA.withPseudoAndKeyColsInValue(false, ROWPARTITION_ROWOFFSET_ENABLED)
        ),
        ROWPARTITION_ROWOFFSET_ENABLED
    );
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldGetWindowedSourceSchemasPostAggregate() {
    // Given:
    analysis.addDataSource(ALIAS, dataSource);

    givenWindowedSource(dataSource);

    // When:
    analysis.getFromSourceSchemas(true);

    // Then:
    verify(sourceSchemasFactory).apply(
        ImmutableMap.of(
            ALIAS,
            SOURCE_SCHEMA.withPseudoAndKeyColsInValue(true, ROWPARTITION_ROWOFFSET_ENABLED)
        ),
        ROWPARTITION_ROWOFFSET_ENABLED
    );
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldGetWindowedGroupBySourceSchemasPostAggregate() {
    // Given:
    analysis.addDataSource(ALIAS, dataSource);

    givenNoneWindowedSource(dataSource);
    analysis.setWindowExpression(windowExpression);

    // When:
    analysis.getFromSourceSchemas(true);

    // Then:
    verify(sourceSchemasFactory).apply(
        ImmutableMap.of(
            ALIAS,
            SOURCE_SCHEMA.withPseudoAndKeyColsInValue(true, ROWPARTITION_ROWOFFSET_ENABLED)
        ),
        ROWPARTITION_ROWOFFSET_ENABLED
    );
  }

  private static void givenNoneWindowedSource(final KsqlStream<?> dataSource) {
    final KsqlTopic topic = mock(KsqlTopic.class);
    when(topic.getKeyFormat()).thenReturn(KeyFormat.nonWindowed(A_FORMAT, SerdeFeatures.of()));
    when(dataSource.getKsqlTopic()).thenReturn(topic);
  }

  private static void givenWindowedSource(final KsqlStream<?> dataSource) {
    final KsqlTopic topic = mock(KsqlTopic.class);
    when(topic.getKeyFormat()).thenReturn(
        KeyFormat.windowed(A_FORMAT, SerdeFeatures.of(), A_WINDOW));
    when(dataSource.getKsqlTopic()).thenReturn(topic);
  }
}
