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

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SchemaKTable extends SchemaKStream {

  private static final Logger log = LoggerFactory.getLogger(SchemaKTable.class);

  private final KTable ktable;
  private final boolean isWindowed;

  public SchemaKTable(final Schema schema, final KTable ktable, final Field keyField,
                      final List<SchemaKStream> sourceSchemaKStreams, boolean isWindowed,
                      Type type) {
    super(schema, null, keyField, sourceSchemaKStreams, type);
    this.ktable = ktable;
    this.isWindowed = isWindowed;
  }

  @Override
  public SchemaKTable into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe,
                           Set<Integer> rowkeyIndexes, KsqlConfig ksqlConfig, KafkaTopicClient kafkaTopicClient) {

    createSinkTopic(kafkaTopicName, ksqlConfig, kafkaTopicClient);

    if (isWindowed) {
      ktable.toStream()
          .map((KeyValueMapper<Windowed<String>, GenericRow, KeyValue<Windowed<String>, GenericRow>>) (key, row) -> {
            if (row == null) {
              return new KeyValue<>(key, null);
            }
            List columns = new ArrayList();
            for (int i = 0; i < row.getColumns().size(); i++) {
              if (!rowkeyIndexes.contains(i)) {
                columns.add(row.getColumns().get(i));
              }
            }
            return new KeyValue<>(key, new GenericRow(columns));
          }).to(new WindowedSerde(), topicValueSerDe, kafkaTopicName);
    } else {
      ktable.toStream()
          .map((KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>) (key, row) -> {
            if (row == null) {
              return new KeyValue<>(key, null);
            }
            List columns = new ArrayList();
            for (int i = 0; i < row.getColumns().size(); i++) {
              if (!rowkeyIndexes.contains(i)) {
                columns.add(row.getColumns().get(i));
              }
            }
            return new KeyValue<>(key, new GenericRow(columns));
          }).to(Serdes.String(), topicValueSerDe, kafkaTopicName);
    }

    return this;
  }

  @Override
  public QueuedSchemaKStream toQueue(Optional<Integer> limit) {
    return new QueuedSchemaKStream(this, limit);
  }

  @Override
  public SchemaKTable filter(final Expression filterExpression) throws Exception {
    SqlPredicate predicate = new SqlPredicate(filterExpression, schema, isWindowed);
    KTable filteredKTable = ktable.filter(predicate.getPredicate());
    return new SchemaKTable(schema, filteredKTable, keyField, Arrays.asList(this), isWindowed,
                            Type.FILTER);
  }

  @Override
  public SchemaKTable select(final List<Pair<String, Expression>> expressionPairList) throws Exception {
    CodeGenRunner codeGenRunner = new CodeGenRunner();
    // TODO: Optimize to remove the code gen for constants and single
    // TODO: columns references and use them directly.
    // TODO: Only use code get when we have real expression.
    List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Pair<String, Expression> expressionPair : expressionPairList) {
      ExpressionMetadata
          expressionEvaluator =
          codeGenRunner.buildCodeGenFromParseTree(expressionPair.getRight(), schema);
      schemaBuilder.field(expressionPair.getLeft(), expressionEvaluator.getExpressionType());
      expressionEvaluators.add(expressionEvaluator);
    }

    KTable projectedKTable = ktable.mapValues((ValueMapper<GenericRow, GenericRow>) row -> {
      try {
        List<Object> newColumns = new ArrayList();
        for (int i = 0; i < expressionPairList.size(); i++) {
          try {
            int[] parameterIndexes = expressionEvaluators.get(i).getIndexes();
            Kudf[] kudfs = expressionEvaluators.get(i).getUdfs();
            Object[] parameterObjects = new Object[parameterIndexes.length];
            for (int j = 0; j < parameterIndexes.length; j++) {
              if (parameterIndexes[j] < 0) {
                parameterObjects[j] = kudfs[j];
              } else {
                parameterObjects[j] =
                    genericRowValueTypeEnforcer.enforceFieldType(parameterIndexes[j],
                                                                 row.getColumns()
                                                                     .get(parameterIndexes[j]));
              }
            }
            Object columnValue = null;
            columnValue = expressionEvaluators.get(i).getExpressionEvaluator()
                .evaluate(parameterObjects);
            newColumns.add(columnValue);
          } catch (Exception e) {
            log.error("Error calculating column with index " + i + " : " +
                      expressionPairList.get(i).getLeft());
            newColumns.add(null);
          }
        }
        GenericRow newRow = new GenericRow(newColumns);
        return newRow;
      } catch (Exception e) {
        log.error("Projection exception for row: " + row.toString());
        log.error(e.getMessage(), e);
        throw new KsqlException("Error in SELECT clause: " + e.getMessage(), e);
      }
    });

    return new SchemaKTable(schemaBuilder.build(), projectedKTable, keyField,
                            Arrays.asList(this), isWindowed, Type.PROJECT);
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
