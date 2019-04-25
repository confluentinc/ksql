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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SchemaKTable<K> extends SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final KTable<K, GenericRow> ktable;

  public SchemaKTable(
      final Schema schema,
      final KTable<K, GenericRow> ktable,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final SerdeFactory<K> keySerdeFactory,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final QueryContext queryContext
  ) {
    this(
        schema,
        ktable,
        keyField,
        sourceSchemaKStreams,
        keySerdeFactory,
        type,
        ksqlConfig,
        functionRegistry,
        StreamsFactories.create(ksqlConfig),
        queryContext
    );
  }

  SchemaKTable(
      final Schema schema,
      final KTable<K, GenericRow> ktable,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final SerdeFactory<K> keySerdeFactory,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final StreamsFactories streamsFactories,
      final QueryContext queryContext
  ) {
    super(
        schema,
        null,
        keyField,
        sourceSchemaKStreams,
        keySerdeFactory,
        type,
        ksqlConfig,
        functionRegistry,
        streamsFactories,
        queryContext
    );
    this.ktable = ktable;
  }

  @Override
  public SchemaKTable<K> into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final Set<Integer> rowkeyIndexes
  ) {

    ktable.toStream()
        .mapValues(row -> {
              if (row == null) {
                return null;
              }
              final List<Object> columns = new ArrayList<>();
              for (int i = 0; i < row.getColumns().size(); i++) {
                if (!rowkeyIndexes.contains(i)) {
                  columns.add(row.getColumns().get(i));
                }
              }
              return new GenericRow(columns);
            }
        ).to(kafkaTopicName, Produced.with(keySerdeFactory.create(), topicValueSerDe));

    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    final SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        schema,
        hasWindowedKey(),
        ksqlConfig,
        functionRegistry,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(Type.FILTER.name()).getQueryContext()))
    );
    final KTable filteredKTable = ktable.filter(predicate.getPredicate());
    return new SchemaKTable<>(
        schema,
        filteredKTable,
        keyField,
        Collections.singletonList(this),
        keySerdeFactory,
        Type.FILTER,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  @Override
  public SchemaKTable<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    final Selection selection = new Selection(
        selectExpressions,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(Type.PROJECT.name()).getQueryContext()))
    );
    return new SchemaKTable<>(
        selection.getProjectedSchema(),
        ktable.mapValues(selection.getSelectValueMapper()),
        selection.getKey(),
        Collections.singletonList(this),
        keySerdeFactory,
        Type.PROJECT,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  @SuppressWarnings("unchecked") // needs investigating
  @Override
  public KStream getKstream() {
    return ktable.toStream();
  }

  public KTable getKtable() {
    return ktable;
  }

  @Override
  public SchemaKGroupedStream groupBy(
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker) {

    final GroupBy groupBy = new GroupBy(groupByExpressions);

    final KGroupedTable kgroupedTable = ktable
        .filter((key, value) -> value != null)
        .groupBy(
            (key, value) -> new KeyValue<>(groupBy.mapper.apply(key, value), value),
            streamsFactories.getGroupedFactory().create(
                StreamsUtil.buildOpName(
                    contextStacker.getQueryContext()), Serdes.String(), valSerde)
        );

    final Field legacyKeyField = new Field(
        groupBy.aggregateKeyName, -1, Schema.OPTIONAL_STRING_SCHEMA);

    final Optional<String> newKeyField = SchemaUtil.getFieldByName(schema, groupBy.aggregateKeyName)
        .map(Field::name);

    return new SchemaKGroupedTable(
        schema,
        kgroupedTable,
        KeyField.of(newKeyField, Optional.of(legacyKeyField)),
        Collections.singletonList(this),
        ksqlConfig,
        functionRegistry);
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final KeyField joinKey,
      final QueryContext.Stacker contextStacker
  ) {
    final KTable<K, GenericRow> joinedKTable = ktable.join(
        schemaKTable.getKtable(),
        new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
    );

    return new SchemaKTable<>(
        joinSchema,
        joinedKTable,
        joinKey,
        ImmutableList.of(this, schemaKTable),
        keySerdeFactory,
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final KeyField joinKey,
      final QueryContext.Stacker contextStacker
  ) {
    final KTable<K, GenericRow> joinedKTable =
        ktable.leftJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable<>(
        joinSchema,
        joinedKTable,
        joinKey,
        ImmutableList.of(this, schemaKTable),
        keySerdeFactory,
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final KeyField joinKey,
      final QueryContext.Stacker contextStacker
  ) {
    final KTable<K, GenericRow> joinedKTable =
        ktable.outerJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable<>(
        joinSchema,
        joinedKTable,
        joinKey,
        ImmutableList.of(this, schemaKTable),
        keySerdeFactory,
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }
}
