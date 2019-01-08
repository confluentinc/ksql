/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.streams.GroupedFactory;
import io.confluent.ksql.streams.JoinedFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Serde<K> keySerde,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        schema,
        ktable,
        keyField,
        sourceSchemaKStreams,
        keySerde,
        type,
        ksqlConfig,
        functionRegistry,
        GroupedFactory.create(ksqlConfig),
        JoinedFactory.create(ksqlConfig)
    );
  }

  SchemaKTable(
      final Schema schema,
      final KTable<K, GenericRow> ktable,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Serde<K> keySerde,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final GroupedFactory groupedFactory,
      final JoinedFactory joinedFactory
  ) {
    super(
        schema,
        null,
        keyField,
        sourceSchemaKStreams,
        keySerde,
        type,
        ksqlConfig,
        functionRegistry,
        groupedFactory,
        joinedFactory
    );
    this.ktable = ktable;
  }

  @SuppressWarnings("unchecked")
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
        ).to(kafkaTopicName, Produced.with(keySerde, topicValueSerDe));

    return this;
  }

  @Override
  public QueuedSchemaKStream toQueue() {
    return new QueuedSchemaKStream<>(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<K> filter(final Expression filterExpression) {
    final SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        schema,
        hasWindowedKey(),
        ksqlConfig,
        functionRegistry
    );
    final KTable filteredKTable = ktable.filter(predicate.getPredicate());
    return new SchemaKTable<>(
        schema,
        filteredKTable,
        keyField,
        Collections.singletonList(this),
        keySerde,
        Type.FILTER,
        ksqlConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> select(final List<SelectExpression> selectExpressions) {
    final Selection selection = new Selection(selectExpressions);
    return new SchemaKTable<>(
        selection.getSchema(),
        ktable.mapValues(selection.getSelectValueMapper()),
        selection.getKey(),
        Collections.singletonList(this),
        keySerde,
        Type.PROJECT,
        ksqlConfig,
        functionRegistry
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
      final String opName) {

    final GroupBy groupBy = new GroupBy(groupByExpressions);

    final KGroupedTable kgroupedTable = ktable
        .filter((key, value) -> value != null)
        .groupBy((key, value) -> new KeyValue<>(groupBy.mapper.apply(key, value), value),
            groupedFactory.create(opName, Serdes.String(), valSerde));

    final Field newKeyField = new Field(
        groupBy.aggregateKeyName, -1, Schema.OPTIONAL_STRING_SCHEMA);
    return new SchemaKGroupedTable(
        schema,
        kgroupedTable,
        newKeyField,
        Collections.singletonList(this),
        ksqlConfig,
        functionRegistry);
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey
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
        keySerde,
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey
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
        keySerde,
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey
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
        keySerde,
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }
}
