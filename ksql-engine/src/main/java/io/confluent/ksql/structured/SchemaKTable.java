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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
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
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SchemaKTable extends SchemaKStream {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final KTable<?, GenericRow> ktable;
  private final boolean isWindowed;

  public SchemaKTable(
      final Schema schema,
      final KTable<?, GenericRow> ktable,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final boolean isWindowed,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    super(
        schema,
        null,
        keyField,
        sourceSchemaKStreams,
        type,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient
    );
    this.ktable = ktable;
    this.isWindowed = isWindowed;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final Set<Integer> rowkeyIndexes
  ) {
    if (isWindowed) {
      final Serde<Windowed<String>> windowedSerde
              = WindowedSerdes.timeWindowedSerdeFrom(String.class);
      ((KTable<Windowed<String>, GenericRow>)ktable).toStream()
          .mapValues(
              row -> {
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
          ).to(kafkaTopicName, Produced.with(windowedSerde, topicValueSerDe));
    } else {
      ((KTable<String, GenericRow>)ktable).toStream()
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
          }).to(kafkaTopicName, Produced.with(Serdes.String(), topicValueSerDe));
    }

    return this;
  }

  @Override
  public QueuedSchemaKStream toQueue() {
    return new QueuedSchemaKStream(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable filter(final Expression filterExpression) {
    final SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        schema,
        isWindowed,
        ksqlConfig,
        functionRegistry
    );
    final KTable filteredKTable = ktable.filter(predicate.getPredicate());
    return new SchemaKTable(
        schema,
        filteredKTable,
        keyField,
        Collections.singletonList(this),
        isWindowed,
        Type.FILTER,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable select(final List<SelectExpression> selectExpressions) {
    final Selection selection = new Selection(selectExpressions, functionRegistry, this);
    return new SchemaKTable(
        selection.getSchema(),
        ktable.mapValues(selection.getSelectValueMapper()),
        selection.getKey(),
        Collections.singletonList(this),
        isWindowed,
        Type.PROJECT,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient
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

  public boolean isWindowed() {
    return isWindowed;
  }

  @Override
  public SchemaKGroupedStream groupBy(
      final Serde<String> keySerde,
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions) {
    final String aggregateKeyName = keyNameForGroupBy(groupByExpressions);
    final List<Integer> newKeyIndexes = keyIndexesForGroupBy(getSchema(), groupByExpressions);

    final KGroupedTable kgroupedTable = ktable.filter((key, value) -> value != null).groupBy(
        (key, value) ->
            new KeyValue<>(buildGroupByKey(newKeyIndexes, value), value),
        Grouped.with(keySerde, valSerde));

    final Field newKeyField = new Field(aggregateKeyName, -1, Schema.OPTIONAL_STRING_SCHEMA);
    return new SchemaKGroupedTable(
        schema,
        kgroupedTable,
        newKeyField,
        Collections.singletonList(this),
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient);
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable join(
      final SchemaKTable schemaKTable,
      final Schema joinSchema,
      final Field joinKey
  ) {

    final KTable joinedKTable =
        ktable.join(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable(
        joinSchema,
        joinedKTable,
        joinKey,
        ImmutableList.of(this, schemaKTable),
        false,
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable leftJoin(
      final SchemaKTable schemaKTable,
      final Schema joinSchema,
      final Field joinKey
  ) {

    final KTable joinedKTable =
        ktable.leftJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable(
        joinSchema,
        joinedKTable,
        joinKey,
        ImmutableList.of(this, schemaKTable),
        false,
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable outerJoin(
      final SchemaKTable schemaKTable,
      final Schema joinSchema,
      final Field joinKey
  ) {

    final KTable joinedKTable =
        ktable.outerJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema())
        );

    return new SchemaKTable(
        joinSchema,
        joinedKTable,
        joinKey,
        ImmutableList.of(this, schemaKTable),
        false,
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        schemaRegistryClient
    );
  }

}
