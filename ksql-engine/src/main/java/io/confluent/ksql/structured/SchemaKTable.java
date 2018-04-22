/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.structured;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.Pair;

public class SchemaKTable extends SchemaKStream {


  private final KTable ktable;
  private final boolean isWindowed;

  public SchemaKTable(
      final Schema schema,
      final KTable ktable,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      boolean isWindowed,
      Type type,
      final FunctionRegistry functionRegistry,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    super(
        schema,
        null,
        keyField,
        sourceSchemaKStreams,
        type,
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
      Set<Integer> rowkeyIndexes
  ) {
    if (isWindowed) {
      final Serde<Windowed<String>> windowedSerde
              = WindowedSerdes.timeWindowedSerdeFrom(String.class);
      ktable.toStream()
          .mapValues(
              (ValueMapper<GenericRow, GenericRow>) row -> {
                if (row == null) {
                  return null;
                }
                List columns = new ArrayList();
                for (int i = 0; i < row.getColumns().size(); i++) {
                  if (!rowkeyIndexes.contains(i)) {
                    columns.add(row.getColumns().get(i));
                  }
                }
                return new GenericRow(columns);
              }
          ).to(kafkaTopicName, Produced.with(windowedSerde, topicValueSerDe));
    } else {
      ktable.toStream()
          .mapValues((ValueMapper<GenericRow, GenericRow>) row -> {
            if (row == null) {
              return null;
            }
            List columns = new ArrayList();
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
  public QueuedSchemaKStream toQueue(Optional<Integer> limit) {
    return new QueuedSchemaKStream(this, limit);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable filter(final Expression filterExpression) {
    SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        schema,
        isWindowed,
        functionRegistry
    );
    KTable filteredKTable = ktable.filter(predicate.getPredicate());
    return new SchemaKTable(
        schema,
        filteredKTable,
        keyField,
        Arrays.asList(this),
        isWindowed,
        Type.FILTER,
        functionRegistry,
        schemaRegistryClient
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable select(final List<Pair<String, Expression>> expressionPairList) {

    final Pair<Schema, SelectValueMapper> schemaAndMapper
        = createSelectValueMapperAndSchema(expressionPairList);

    KTable projectedKTable = ktable.mapValues(schemaAndMapper.right);

    return new SchemaKTable(
        schemaAndMapper.left,
        projectedKTable,
        keyField,
        Collections.singletonList(this),
        isWindowed,
        Type.PROJECT,
        functionRegistry,
        schemaRegistryClient
    );
  }

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

}
