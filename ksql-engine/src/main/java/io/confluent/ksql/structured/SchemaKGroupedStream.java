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
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

public class SchemaKGroupedStream {

  final Schema schema;
  final KGroupedStream kgroupedStream;
  final Optional<Field> keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final MaterializedFactory materializedFactory;

  SchemaKGroupedStream(
      final Schema schema,
      final KGroupedStream kgroupedStream,
      final Optional<Field> keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        schema,
        kgroupedStream,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry,
        MaterializedFactory.create(ksqlConfig)
    );
  }

  SchemaKGroupedStream(
      final Schema schema,
      final KGroupedStream kgroupedStream,
      final Optional<Field> keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final MaterializedFactory materializedFactory
  ) {
    this.schema = schema;
    this.kgroupedStream = kgroupedStream;
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = functionRegistry;
    this.materializedFactory = materializedFactory;
  }

  public Optional<Field> getKeyField() {
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
    final SerdeFactory<?> keySerdeFactory;
    if (windowExpression != null) {
      keySerdeFactory = getKeySerde(windowExpression);
      table = aggregateWindowed(
          initializer,
          aggValToFunctionMap,
          aggValToValColumnMap,
          windowExpression,
          topicValueSerDe,
          contextStacker);
    } else {
      keySerdeFactory = (SerdeFactory)Serdes::String;

      table = aggregateNonWindowed(
          initializer,
          aggValToFunctionMap,
          aggValToValColumnMap,
          topicValueSerDe,
          contextStacker);
    }

    return new SchemaKTable(
        schema,
        table,
        keyField,
        sourceSchemaKStreams,
        keySerdeFactory,
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

    final Materialized<String, GenericRow, KeyValueStore<Bytes, byte[]>> materialized
          = materializedFactory.create(
              Serdes.String(),
              topicValueSerDe,
              StreamsUtil.buildOpName(contextStacker.getQueryContext()));
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

    final Materialized<String, GenericRow, WindowStore<Bytes, byte[]>> materialized
          = materializedFactory.create(
              Serdes.String(),
              topicValueSerDe,
              StreamsUtil.buildOpName(contextStacker.getQueryContext()));
    final KTable aggKtable = ksqlWindowExpression.applyAggregate(
        kgroupedStream, initializer, aggregator, materialized);

    final WindowSelectMapper windowSelectMapper = new WindowSelectMapper(indexToFunctionMap);
    if (!windowSelectMapper.hasSelects()) {
      return aggKtable;
    }

    return aggKtable.mapValues((readOnlyKey, value) ->
        windowSelectMapper.apply((Windowed<?>) readOnlyKey, (GenericRow) value));
  }

  private SerdeFactory<Windowed<String>> getKeySerde(final WindowExpression windowExpression) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)) {
      return () -> WindowedSerdes.timeWindowedSerdeFrom(String.class);
    }

    return windowExpression.getKsqlWindowExpression().getKeySerdeFactory(String.class);
  }
}
