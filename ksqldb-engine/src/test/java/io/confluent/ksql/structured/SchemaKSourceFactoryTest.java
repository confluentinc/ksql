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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
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

  private static final SerdeOptions SERDE_OPTIONS = SerdeOptions
      .of(SerdeOption.UNWRAP_SINGLE_VALUES);

  private static final String TOPIC_NAME = "fred";
  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  @Mock
  private KsqlQueryBuilder builder;
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

  @Before
  public void setUp() {
    when(dataSource.getSerdeOptions()).thenReturn(SERDE_OPTIONS);
    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(dataSource.getSchema()).thenReturn(SCHEMA);

    when(topic.getKeyFormat()).thenReturn(keyFormat);
    when(topic.getValueFormat()).thenReturn(valueFormat);

    when(contextStacker.getQueryContext()).thenReturn(queryContext);

    when(builder.getKsqlConfig()).thenReturn(CONFIG);
    when(builder.getFunctionRegistry()).thenReturn(functionRegistry);

    when(keyFormat.getFormatInfo()).thenReturn(keyFormatInfo);
    when(valueFormat.getFormatInfo()).thenReturn(valueFormatInfo);
  }

  @Test
  public void shouldBuildWindowedStream() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(keyFormat.isWindowed()).thenReturn(true);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
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
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(keyFormat.isWindowed()).thenReturn(false);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.empty());

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
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
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(keyFormat.isWindowed()).thenReturn(true);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
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
  public void shouldBuildNonWindowedTable() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(keyFormat.isWindowed()).thenReturn(false);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.empty());

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
        dataSource,
        contextStacker
    );

    // Then:
    assertThat(result, instanceOf(SchemaKTable.class));
    assertThat(result.getSourceStep(), instanceOf(TableSource.class));

    assertValidSchema(result);
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  private void assertValidSchema(final SchemaKStream<?> result) {
    assertThat(
        result.getSchema(),
        is(new StepSchemaResolver(CONFIG, functionRegistry).resolve(result.getSourceStep(), SCHEMA))
    );
  }
}