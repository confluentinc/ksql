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
import io.confluent.ksql.serde.KeySerde;
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
  final LogicalSchema schema;
  final KeySerde<Struct> keySerde;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final MaterializedFactory materializedFactory;

  SchemaKGroupedStream(
      final KGroupedStream kgroupedStream,
      final LogicalSchema schema,
      final KeySerde<Struct> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        kgroupedStream, schema,
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
      final LogicalSchema schema,
      final KeySerde<Struct> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final MaterializedFactory materializedFactory
  ) {
    this.kgroupedStream = kgroupedStream;
    this.schema = schema;
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

  @SuppressWarnings("unchecked")
  public SchemaKTable<?> aggregate(
      final Initializer initializer,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final Map<Integer, Integer> aggValToValColumnMap,
      final WindowExpression windowExpression,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker) {

    final KTable table;
    final KeySerde<?> newKeySerde;
    if (windowExpression != null) {
      newKeySerde = getKeySerde(windowExpression);
      table = aggregateWindowed(
          initializer,
          aggValToFunctionMap,
          aggValToValColumnMap,
          windowExpression,
          topicValueSerDe,
          contextStacker);
    } else {
      newKeySerde = keySerde;

      table = aggregateNonWindowed(
          initializer,
          aggValToFunctionMap,
          aggValToValColumnMap,
          topicValueSerDe,
          contextStacker);
    }

    return new SchemaKTable(
        table,
        schema,
        newKeySerde,
        keyField,
        sourceSchemaKStreams,
        SchemaKStream.Type.AGGREGATE,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext());
  }

  @SuppressWarnings("unchecked")
  private KTable aggregateNonWindowed(
      final Initializer initializer,
      final Map<Integer, KsqlAggregateFunction> indexToFunctionMap,
      final Map<Integer, Integer> indexToValueMap,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker) {

    final UdafAggregator aggregator = new KudafAggregator(
        indexToFunctionMap, indexToValueMap);

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
      final Map<Integer, KsqlAggregateFunction> indexToFunctionMap,
      final Map<Integer, Integer> indexToValueMap,
      final WindowExpression windowExpression,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker) {

    final UdafAggregator aggregator = new KudafAggregator(
        indexToFunctionMap, indexToValueMap);

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

  private KeySerde<Windowed<Struct>> getKeySerde(final WindowExpression windowExpression) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)) {
      return keySerde.rebind(WindowInfo.of(
          WindowType.TUMBLING,
          Optional.of(Duration.ofMillis(Long.MAX_VALUE))
      ));
    }

    return keySerde.rebind(windowExpression.getKsqlWindowExpression().getWindowInfo());
  }
}
