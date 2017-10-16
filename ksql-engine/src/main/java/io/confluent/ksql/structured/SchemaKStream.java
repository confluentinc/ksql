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

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SchemaKStream {
  
  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN, TOSTREAM }

  protected final Schema schema;
  protected final KStream kstream;
  final Field keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  private final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  protected final Type type;

  public SchemaKStream(final Schema schema, final KStream kstream, final Field keyField,
                       final List<SchemaKStream> sourceSchemaKStreams, Type type) {
    this.schema = schema;
    this.kstream = kstream;
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.type = type;
  }

  public QueuedSchemaKStream toQueue(Optional<Integer> limit) {
    return new QueuedSchemaKStream(this, limit);
  }

  public SchemaKStream into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe,
                            final Set<Integer> rowkeyIndexes) {

    kstream
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
        }).to(kafkaTopicName, Produced.with(Serdes.String(), topicValueSerDe));
    return this;
  }

  public SchemaKStream filter(final Expression filterExpression) throws Exception {
    SqlPredicate predicate = new SqlPredicate(filterExpression, schema, false);
    KStream filteredKStream = kstream.filter(predicate.getPredicate());
    return new SchemaKStream(schema, filteredKStream, keyField, Arrays.asList(this),
                             Type.FILTER);
  }

  public SchemaKStream select(final Schema selectSchema) {

    KStream
        projectedKStream =
        kstream.map((KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>) (key, row) -> {
          List<Object> newColumns = new ArrayList();
          for (Field schemaField : selectSchema.fields()) {
            newColumns.add(
                row.getColumns().get(SchemaUtil.getFieldIndexByName(schema, schemaField.name())));
          }
          GenericRow newRow = new GenericRow(newColumns);
          return new KeyValue<>(key, newRow);
        });

    return new SchemaKStream(selectSchema, projectedKStream, keyField, Arrays.asList(this),
                             Type.PROJECT);
  }

  public SchemaKStream select(final List<Pair<String, Expression>> expressionPairList)
      throws Exception {
    final Pair<Schema, SelectValueMapper> schemaAndMapper = createSelectValueMapperAndSchema(expressionPairList);

    return new SchemaKStream(schemaAndMapper.left,
        kstream.mapValues(schemaAndMapper.right), keyField, Collections.singletonList(this),
                             Type.PROJECT);
  }

  Pair<Schema, SelectValueMapper> createSelectValueMapperAndSchema(final List<Pair<String, Expression>> expressionPairList) throws Exception {
    final CodeGenRunner codeGenRunner = new CodeGenRunner();
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
    for (Pair<String, Expression> expressionPair : expressionPairList) {
      final ExpressionMetadata
          expressionEvaluator =
          codeGenRunner.buildCodeGenFromParseTree(expressionPair.getRight(), schema);
      schemaBuilder.field(expressionPair.getLeft(), expressionEvaluator.getExpressionType());
      expressionEvaluators.add(expressionEvaluator);
    }
    return new Pair<>(schemaBuilder.build(), new SelectValueMapper(genericRowValueTypeEnforcer,
        expressionPairList,
        expressionEvaluators));
  }

  public SchemaKStream leftJoin(final SchemaKTable schemaKTable, final Schema joinSchema,
                                final Field joinKey,
                                KsqlTopicSerDe joinSerDe) {

    KStream joinedKStream =
        kstream.leftJoin(
            schemaKTable.getKtable(), (ValueJoiner<GenericRow, GenericRow, GenericRow>) (leftGenericRow, rightGenericRow) -> {
              List<Object> columns = new ArrayList<>();
              columns.addAll(leftGenericRow.getColumns());
              if (rightGenericRow == null) {
                for (int i = leftGenericRow.getColumns().size();
                     i < joinSchema.fields().size(); i++) {
                  columns.add(null);
                }
              } else {
                columns.addAll(rightGenericRow.getColumns());
              }

              GenericRow joinGenericRow = new GenericRow(columns);
              return joinGenericRow;
            }, Joined.with(Serdes.String(), SerDeUtil.getRowSerDe(joinSerDe, this.getSchema()), null));

    return new SchemaKStream(joinSchema, joinedKStream, joinKey,
                             Arrays.asList(this, schemaKTable), Type.JOIN);
  }

  public SchemaKStream selectKey(final Field newKeyField) {
    if (keyField != null &&
        keyField.name().equals(newKeyField.name())) {
      return this;
    }

    KStream keyedKStream = kstream.selectKey((KeyValueMapper<String, GenericRow, String>) (key, value) -> {

      String
          newKey =
          value.getColumns().get(SchemaUtil.getFieldIndexByName(schema, newKeyField.name()))
              .toString();
      return newKey;
    }).map((KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>) (key, row) -> {
      row.getColumns().set(SchemaUtil.ROWKEY_NAME_INDEX, key);
      return new KeyValue<>(key, row);
    });

    return new SchemaKStream(schema, keyedKStream, newKeyField, Arrays.asList(this),
                             Type.REKEY);
  }

  public SchemaKGroupedStream groupByKey(final Serde keySerde,
                                         final Serde valSerde) {
    KGroupedStream kgroupedStream = kstream.groupByKey(Serialized.with(keySerde, valSerde));
    return new SchemaKGroupedStream(schema, kgroupedStream, keyField, Arrays.asList(this));
  }

  public Field getKeyField() {
    return keyField;
  }

  public Schema getSchema() {
    return schema;
  }

  public KStream getKstream() {
    return kstream;
  }

  public List<SchemaKStream> getSourceSchemaKStreams() {
    return sourceSchemaKStreams;
  }

  public String getExecutionPlan(String indent) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(indent + " > [ " + type + " ] Schema: " + SchemaUtil
        .getSchemaDefinitionString(schema) + ".\n");
    for (SchemaKStream schemaKStream: sourceSchemaKStreams) {
      stringBuilder.append("\t" + indent + schemaKStream.getExecutionPlan(indent + "\t"));
    }
    return stringBuilder.toString();
  }

}
