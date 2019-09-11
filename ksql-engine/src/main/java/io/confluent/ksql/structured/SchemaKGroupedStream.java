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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdafAggregator;
import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.function.udaf.window.WindowSelectMapper;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;

public class SchemaKGroupedStream {

  final KGroupedStream kgroupedStream;
  final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep;
  final KeyFormat keyFormat;
  final KeySerde<Struct> keySerde;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final MaterializedFactory materializedFactory;

  SchemaKGroupedStream(
      final KGroupedStream kgroupedStream,
      final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep,
      final KeyFormat keyFormat,
      final KeySerde<Struct> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        kgroupedStream,
        sourceStep,
        keyFormat,
        keySerde,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry,
        MaterializedFactory.create(ksqlConfig)
    );
  }

  SchemaKGroupedStream(
      final KGroupedStream kgroupedStream,
      final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep,
      final KeyFormat keyFormat,
      final KeySerde<Struct> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final MaterializedFactory materializedFactory
  ) {
    this.kgroupedStream = kgroupedStream;
    this.sourceStep = sourceStep;
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.keySerde = Objects.requireNonNull(keySerde, "keySerde");
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = functionRegistry;
    this.materializedFactory = materializedFactory;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public ExecutionStep<KGroupedStream<Struct, GenericRow>> getSourceStep() {
    return sourceStep;
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<?> aggregate(
      final LogicalSchema aggregateSchema,
      final Initializer initializer,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final WindowExpression windowExpression,
      final ValueFormat valueFormat,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker
  ) {
    throwOnValueFieldCountMismatch(aggregateSchema, nonFuncColumnCount, aggValToFunctionMap);

    final KTable table;
    final KeySerde<?> newKeySerde;
    final KeyFormat keyFormat;
    if (windowExpression != null) {
      keyFormat = getKeyFormat(windowExpression);
      newKeySerde = getKeySerde(windowExpression);

      table = aggregateWindowed(
          initializer,
          nonFuncColumnCount,
          aggValToFunctionMap,
          windowExpression,
          topicValueSerDe,
          contextStacker
      );
    } else {
      keyFormat = this.keyFormat;
      newKeySerde = keySerde;

      table = aggregateNonWindowed(
          initializer,
          nonFuncColumnCount,
          aggValToFunctionMap,
          topicValueSerDe,
          contextStacker
      );
    }

    final ExecutionStep step = ExecutionStepFactory.streamAggregate(
        contextStacker,
        sourceStep,
        aggregateSchema,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        nonFuncColumnCount,
        aggregations
    );
    return new SchemaKTable(
        table,
        step,
        keyFormat,
        newKeySerde,
        keyField,
        sourceSchemaKStreams,
        SchemaKStream.Type.AGGREGATE,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  private KTable aggregateNonWindowed(
      final Initializer initializer,
      final int nonFuncColumnCount,
      final Map<Integer, KsqlAggregateFunction> indexToFunctionMap,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker
  ) {
    final UdafAggregator aggregator = new KudafAggregator(nonFuncColumnCount, indexToFunctionMap);

    final Materialized<Struct, GenericRow, ?> materialized = materializedFactory.create(
        keySerde,
        topicValueSerDe,
        StreamsUtil.buildOpName(contextStacker.getQueryContext())
    );

    return kgroupedStream.aggregate(initializer, aggregator, materialized);
  }

  @SuppressWarnings("unchecked")
  private KTable aggregateWindowed(
      final Initializer initializer,
      final int nonFuncColumnCount,
      final Map<Integer, KsqlAggregateFunction> indexToFunctionMap,
      final WindowExpression windowExpression,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker
  ) {
    final UdafAggregator aggregator = new KudafAggregator(nonFuncColumnCount, indexToFunctionMap);

    final KsqlWindowExpression ksqlWindowExpression = windowExpression.getKsqlWindowExpression();

    final Materialized<Struct, GenericRow, ?> materialized = materializedFactory.create(
        keySerde,
        topicValueSerDe,
        StreamsUtil.buildOpName(contextStacker.getQueryContext())
    );

    final KTable<Windowed<Struct>, GenericRow> aggKtable = ksqlWindowExpression.applyAggregate(
        kgroupedStream, initializer, aggregator, materialized);

    final WindowSelectMapper windowSelectMapper = new WindowSelectMapper(indexToFunctionMap);
    if (!windowSelectMapper.hasSelects()) {
      return aggKtable;
    }

    return aggKtable.mapValues(windowSelectMapper);
  }

  private KeyFormat getKeyFormat(final WindowExpression windowExpression) {
    return KeyFormat.windowed(
        FormatInfo.of(Format.KAFKA),
        windowExpression.getKsqlWindowExpression().getWindowInfo()
    );
  }

  private KeySerde<Windowed<Struct>> getKeySerde(final WindowExpression windowExpression) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)) {
      return keySerde.rebind(WindowInfo.of(
          WindowType.TUMBLING,
          Optional.of(Duration.ofMillis(Long.MAX_VALUE))
      ));
    }

    return keySerde.rebind(windowExpression.getKsqlWindowExpression().getWindowInfo());
  }

  static void throwOnValueFieldCountMismatch(
      final LogicalSchema aggregateSchema,
      final int nonFuncColumnCount,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap
  ) {
    final int nonAggColumnCount = aggValToFunctionMap.size();
    final int totalColumnCount = nonAggColumnCount + nonFuncColumnCount;

    final int valueColumnCount = aggregateSchema.value().size();
    if (valueColumnCount != totalColumnCount) {
      throw new IllegalArgumentException(
          "Aggregate schema value field count does not match expected."
          + " expected: " + totalColumnCount
          + ", actual: " + valueColumnCount
          + ", schema: " + aggregateSchema
      );
    }
  }
}
