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

package io.confluent.ksql.structured;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaKSourceFactoryTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("FOO"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("BAR"), SqlTypes.STRING)
      .build();

  private static final String TOPIC_NAME = "fred";
  private static final int OLD_PSEUDOCOLUMN_VERSION = SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION;
  private static final int NEW_PSEUDOCOLUMN_VERSION = SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION;

  @Mock
  private PlanBuildContext buildContext;
  @Mock
  private DataSource dataSource;
  @Mock
  private Stacker contextStacker;
  @Mock
  private QueryContext queryContext;
  @Mock
  private KsqlTopic topic;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private FormatInfo keyFormatInfo;
  @Mock
  private ValueFormat valueFormat;
  @Mock
  private FormatInfo valueFormatInfo;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private WindowInfo windowInfo;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private PlanInfo planInfo;
  @Mock
  private StreamSource streamSource;
  @Mock
  private TableSource tableSource;
  @Mock
  private TableSourceV1 tableSourceV1;

  @Before
  public void setUp() {
    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(dataSource.getSchema()).thenReturn(SCHEMA);

    when(topic.getKeyFormat()).thenReturn(keyFormat);
    when(topic.getValueFormat()).thenReturn(valueFormat);

    when(contextStacker.getQueryContext()).thenReturn(queryContext);

    when(buildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);

    when(keyFormat.getFormatInfo()).thenReturn(keyFormatInfo);
    when(keyFormat.getFeatures()).thenReturn(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));
    when(valueFormat.getFormatInfo()).thenReturn(valueFormatInfo);
    when(valueFormat.getFeatures()).thenReturn(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES));
  }

  @Test
  public void shouldBuildWindowedStream() {
    // Given:
    givenWindowedStream();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(result, not(instanceOf(SchemaKTable.class)));
    assertThat(result.getSourceStep(), instanceOf(WindowedStreamSource.class));

    assertValidSchema(result);
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildNonWindowedStream() {
    // Given:
    givenNonWindowedStream();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(result, not(instanceOf(SchemaKTable.class)));
    assertThat(result.getSourceStep(), instanceOf(StreamSource.class));

    assertValidSchema(result);
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildWindowedTable() {
    // Given:
    givenWindowedTable();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(result, instanceOf(SchemaKTable.class));
    assertThat(result.getSourceStep(), instanceOf(WindowedTableSource.class));

    assertValidSchema(result);
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildV1NonWindowedTable() {
    // Given:
    givenNonWindowedTable();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(result, instanceOf(SchemaKTable.class));
    assertThat(result.getSourceStep(), instanceOf(TableSourceV1.class));

    assertValidSchema(result);
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildV2NonWindowedTable() {
    // Given:
    givenNonWindowedTable();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(result, instanceOf(SchemaKTable.class));
    assertThat(result.getSourceStep(), instanceOf(TableSource.class));

    assertValidSchema(result);
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildCorrectFormatsForV2NonWindowedTable() {
    // Given:
    givenNonWindowedTable();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((TableSource) result.getSourceStep()).getStateStoreFormats(), is(
        InternalFormats.of(
            keyFormat,
            valueFormatInfo
        )
    ));
  }

  @Test
  public void shouldReplaceOldNonWindowedStreamSourceWithMatchingPseudoColumnVersion() {
    // Given:
    givenNonWindowedStream();
    givenExistingQueryWithOldPseudoColumnVersion();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((StreamSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(OLD_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldCreateNonWindowedStreamSourceWithNewPseudoColumnVersionIfNoOldQuery() {
    // Given:
    givenNonWindowedStream();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((StreamSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(NEW_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldReplaceOldWindowedStreamSourceWithMatchingPseudoColumnVersion() {
    // Given:
    givenWindowedStream();
    givenExistingQueryWithOldPseudoColumnVersion();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((WindowedStreamSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(OLD_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldCreateWindowedStreamSourceWithNewPseudoColumnVersionIfNoOldQuery() {
    // Given:
    givenWindowedStream();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((WindowedStreamSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(NEW_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldCreateNonWindowedTableSourceV2WithNewPseudoColumnVersionIfNoOldQuery() {
    // Given:
    givenNonWindowedTable();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((TableSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(NEW_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldCreateNonWindowedTableSourceV1WithOldPseudoColumnVersionIfNoOldQuery() {
    // Given:
    givenNonWindowedTable();
    givenOnOldPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((TableSourceV1) result.getSourceStep()).getPseudoColumnVersion(), equalTo(OLD_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldReplaceTableSourceV1WithSame() {
    // Given:
    givenNonWindowedTable();
    givenExistingTableSourceV1();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((TableSourceV1) result.getSourceStep()).getPseudoColumnVersion(), equalTo(OLD_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldReplaceOldWindowedTableSourceWithMatchingPseudoColumnVersion() {
    // Given:
    givenWindowedTable();
    givenExistingQueryWithOldPseudoColumnVersion();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((WindowedTableSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(OLD_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }

  @Test
  public void shouldCreateWindowedTableSourceWithNewPseudoColumnVersionIfNoOldQuery() {
    // Given:
    givenWindowedTable();
    givenOnNewPseudoColumnVersion();

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        buildContext,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(((WindowedTableSource) result.getSourceStep()).getPseudoColumnVersion(), equalTo(NEW_PSEUDOCOLUMN_VERSION));
    assertValidSchema(result);
  }



  @Test
  public void shouldThrowOnV1TableSourceWithPseudoColumnVersionGreaterThanZero() {
    // Given:
    givenNonWindowedTable();
    givenExistingTableSourceV1();
    when(tableSourceV1.getPseudoColumnVersion()).thenReturn(NEW_PSEUDOCOLUMN_VERSION);

    // When:
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> SchemaKSourceFactory.buildSource(
            buildContext,
            dataSource,
            contextStacker
        )
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("TableSourceV2 was released in conjunction with pseudocolumnversion 1. Something has gone very wrong")
    );
  }

  private void givenNonWindowedStream() {
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.empty());
    when(keyFormat.isWindowed()).thenReturn(false);
  }

  private void givenWindowedStream() {
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(keyFormat.isWindowed()).thenReturn(true);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));
  }

  private void givenNonWindowedTable() {
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(keyFormat.getFormatInfo().getFormat()).thenReturn("JSON");
    when(keyFormat.isWindowed()).thenReturn(false);
  }

  private void givenWindowedTable() {
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(keyFormat.isWindowed()).thenReturn(true);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));
  }

  private void givenExistingTableSourceV1() {
    givenExistingQueryWithOldPseudoColumnVersion();
    when(buildContext.getPlanInfo()).thenReturn(Optional.of(planInfo));
    when(planInfo.getSourceSet()).thenReturn(ImmutableSet.of(tableSourceV1));
    when(tableSource.getPseudoColumnVersion()).thenReturn(OLD_PSEUDOCOLUMN_VERSION);
  }

  private void givenExistingQueryWithOldPseudoColumnVersion() {
    when(buildContext.getPlanInfo()).thenReturn(Optional.of(planInfo));
    when(planInfo.getSourceSet()).thenReturn(ImmutableSet.of(streamSource));
    when(streamSource.getPseudoColumnVersion()).thenReturn(OLD_PSEUDOCOLUMN_VERSION);
  }

  private void givenOnNewPseudoColumnVersion() {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);
  }

  private void givenOnOldPseudoColumnVersion() {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(false);
  }

  private void assertValidSchema(final SchemaKStream<?> result) {
    assertThat(
        result.getSchema(),
        is(new StepSchemaResolver(ksqlConfig, functionRegistry).resolve(result.getSourceStep(), SCHEMA))
    );
  }
}