/*
 * Copyright 2018 Confluent Inc.
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedStreamTest {
  private static final LogicalSchema IN_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN1"), SqlTypes.INTEGER)
      .build();
  private static final LogicalSchema AGG_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("AGG0"), SqlTypes.BIGINT)
      .build();
  private static final LogicalSchema OUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUT0"), SqlTypes.STRING)
      .build();
  private static final FunctionCall AGG = new FunctionCall(
      FunctionName.of("SUM"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.of(ColumnName.of("IN1"))))
  );
  private static final KsqlWindowExpression KSQL_WINDOW_EXP = new SessionWindowExpression(
      100, TimeUnit.SECONDS
  );

  @Mock
  private KGroupedStream groupedStream;
  @Mock
  private SessionWindowedKStream sessionWindowedStream;
  @Mock
  private KeyField keyField;
  @Mock
  private List<SchemaKStream> sourceStreams;
  @Mock
  private KsqlConfig config;
  @Mock
  private Serde<GenericRow> topicValueSerDe;
  @Mock
  private FunctionCall aggCall;
  @Mock
  private KTable table;
  @Mock
  private WindowExpression windowExp;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Materialized materialized;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private KeySerde<Windowed<Struct>> windowedKeySerde;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ValueFormat valueFormat;
  @Mock
  private KsqlQueryBuilder builder;

  private final FunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");

  private SchemaKGroupedStream schemaGroupedStream;

  @Before
  public void setUp() {
    schemaGroupedStream = new SchemaKGroupedStream(
        groupedStream,
        sourceStep,
        keyFormat,
        keySerde,
        keyField,
        sourceStreams,
        config,
        functionRegistry,
        materializedFactory
    );
    when(sourceStep.getSchema()).thenReturn(IN_SCHEMA);
    when(windowExp.getKsqlWindowExpression()).thenReturn(KSQL_WINDOW_EXP);
    when(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)).thenReturn(false);
    when(materializedFactory.create(any(), any(), any())).thenReturn(materialized);
    when(builder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(builder.buildValueSerde(any(), any(), any())).thenReturn(topicValueSerDe);
    when(builder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(keySerde.rebind(any(WindowInfo.class))).thenReturn(windowedKeySerde);
    when(table.mapValues(any(ValueMapper.class))).thenReturn(table);
  }

  @Test
  public void shouldReturnKTableWithOutputSchema() {
    // Given:
    when(groupedStream.aggregate(any(), any(), any())).thenReturn(table);

    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(AGG),
        Optional.empty(),
        valueFormat,
        queryContext,
        builder
    );

    // Then:
    assertThat(result.getSchema(), is(OUT_SCHEMA));
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // Given:
    when(groupedStream.aggregate(any(), any(), any())).thenReturn(table);

    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(AGG),
        Optional.empty(),
        valueFormat,
        queryContext,
        builder
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamAggregate(
                queryContext,
                schemaGroupedStream.getSourceStep(),
                OUT_SCHEMA,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(AGG),
                AGG_SCHEMA
            )
        )
    );
    assertThat(result.getKtable(), is(table));
  }

  @Test
  public void shouldBuildStepForWindowedAggregate() {
    // Given:
    when(groupedStream.windowedBy(any(SessionWindows.class))).thenReturn(sessionWindowedStream);
    when(sessionWindowedStream.aggregate(any(), any(), any(), any())).thenReturn(table);
    when(table.mapValues(any(ValueMapper.class))).thenReturn(table);

    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(AGG),
        Optional.of(windowExp),
        valueFormat,
        queryContext,
        builder
    );

    // Then:
    final KeyFormat expected = KeyFormat.windowed(
        FormatInfo.of(Format.KAFKA),
        WindowInfo.of(WindowType.SESSION, Optional.empty())
    );
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamWindowedAggregate(
                queryContext,
                schemaGroupedStream.getSourceStep(),
                OUT_SCHEMA,
                Formats.of(expected, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(AGG),
                AGG_SCHEMA,
                KSQL_WINDOW_EXP
            )
        )
    );
    assertThat(result.getKtable(), is(table));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnColumnCountMismatch() {
    // When:
    schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        2,
        ImmutableList.of(aggCall),
        Optional.of(windowExp),
        valueFormat,
        queryContext,
        builder
    );
  }
}
