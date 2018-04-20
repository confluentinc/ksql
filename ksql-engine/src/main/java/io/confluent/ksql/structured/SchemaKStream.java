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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import org.codehaus.commons.compiler.CompileException;


public class SchemaKStream {
  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN, TOSTREAM }

  private static String GROUP_BY_COLUMN_SEPARATOR = "|+|";

  final Schema schema;
  final KStream<String, GenericRow> kstream;
  final Field keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final Type type;
  final FunctionRegistry functionRegistry;
  private OutputNode output;
  final SchemaRegistryClient schemaRegistryClient;


  public SchemaKStream(
      final Schema schema,
      final KStream<String, GenericRow> kstream,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final FunctionRegistry functionRegistry,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    this.schema = schema;
    this.kstream = kstream;
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.type = type;
    this.functionRegistry = functionRegistry;
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public QueuedSchemaKStream toQueue(Optional<Integer> limit) {
    return new QueuedSchemaKStream(this, limit);
  }

  public SchemaKStream into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final Set<Integer> rowkeyIndexes
  ) {

    kstream
        .mapValues(row -> {
          if (row == null) {
            return null;
          }
          List<Object> columns = new ArrayList<>();
          for (int i = 0; i < row.getColumns().size(); i++) {
            if (!rowkeyIndexes.contains(i)) {
              columns.add(row.getColumns().get(i));
            }
          }
          return new GenericRow(columns);
        }).to(kafkaTopicName, Produced.with(Serdes.String(), topicValueSerDe));
    return this;
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream filter(final Expression filterExpression) {
    SqlPredicate predicate = new SqlPredicate(filterExpression, schema, false, functionRegistry);
    KStream<String, GenericRow> filteredKStream = kstream.filter(predicate.getPredicate());
    return new SchemaKStream(
        schema,
        filteredKStream,
        keyField,
        Arrays.asList(this),
        Type.FILTER,
        functionRegistry,
        schemaRegistryClient
    );
  }

  public SchemaKStream select(final List<Pair<String, Expression>> expressionPairList) {
    Selection selection = new Selection(expressionPairList, functionRegistry, this);
    return new SchemaKStream(
        selection.getSchema(),
        kstream.mapValues(selection.getSelectValueMapper()),
        selection.getKey(),
        Collections.singletonList(this),
        Type.PROJECT,
        functionRegistry,
        schemaRegistryClient
    );
  }

  static class Selection {
    private Schema schema;
    private Field key;
    private SelectValueMapper selectValueMapper;

    public Selection(
        final List<Pair<String, Expression>> expressionPairList,
        final FunctionRegistry functionRegistry,
        final SchemaKStream fromStream) {
      key = findKeyField(expressionPairList, fromStream);
      List<ExpressionMetadata> expressionEvaluators = buildExpressions(
          expressionPairList, functionRegistry, fromStream);
      schema = buildSchema(expressionPairList, expressionEvaluators);
      selectValueMapper = new SelectValueMapper(
          new GenericRowValueTypeEnforcer(
              fromStream.getSchema()), expressionPairList, expressionEvaluators);
    }

    private Field findKeyField(
        final List<Pair<String, Expression>> expressionPairList, final SchemaKStream fromStream) {
      if (fromStream.getKeyField() == null) {
        return null;
      }
      if (fromStream.getKeyField().index() == -1) {
        // The key "field" isn't an actual field in the schema
        return fromStream.getKeyField();
      }
      for (int i = 0; i < expressionPairList.size(); i++) {
        String toName = expressionPairList.get(i).left;
        Expression toExpression = expressionPairList.get(i).right;

        if (toExpression instanceof DereferenceExpression) {
          String fromName = ((DereferenceExpression) toExpression).getFieldName();
          if (fromStream.getKeyField().name().equals(fromName)) {
            return new Field(toName, i, fromStream.getKeyField().schema());
          }
        }
      }
      return null;
    }

    private Schema buildSchema(
        final List<Pair<String, Expression>> expressionPairList,
        final List<ExpressionMetadata> expressionEvaluators) {
      final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      IntStream.range(0, expressionPairList.size()).forEach(
          i -> schemaBuilder.field(
              expressionPairList.get(i).getLeft(),
              expressionEvaluators.get(i).getExpressionType()));
      return schemaBuilder.build();
    }

    private ExpressionMetadata buildExpression(CodeGenRunner codeGenRunner, Expression expression) {
      try {
        return codeGenRunner.buildCodeGenFromParseTree(expression);
      } catch (CompileException e) {
        throw new KsqlException("Code generation failed for SelectValueMapper", e);
      } catch (Exception e) {
        throw new RuntimeException("Unexpected error generating code for SelectValueMapper", e);
      }
    }

    private List<ExpressionMetadata> buildExpressions(
        final List<Pair<String, Expression>> expressionPairList,
        final FunctionRegistry functionRegistry,
        final SchemaKStream fromStream) {
      final CodeGenRunner codeGenRunner = new CodeGenRunner(
          fromStream.getSchema(), functionRegistry);
      return expressionPairList.stream()
          .map(Pair::getRight)
          .map(e -> buildExpression(codeGenRunner, e))
          .collect(Collectors.toList());
    }

    public Schema getSchema() {
      return schema;
    }

    public Field getKey() {
      return key;
    }

    public SelectValueMapper getSelectValueMapper() {
      return selectValueMapper;
    }
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream leftJoin(
      final SchemaKTable schemaKTable,
      final Schema joinSchema,
      final Field joinKey,
      KsqlTopicSerDe joinSerDe,
      KsqlConfig ksqlConfig
  ) {

    KStream joinedKStream =
        kstream.leftJoin(
            schemaKTable.getKtable(),
            (ValueJoiner<GenericRow, GenericRow, GenericRow>) (leftGenericRow, rightGenericRow) -> {
              List<Object> columns = new ArrayList<>(leftGenericRow.getColumns());
              if (rightGenericRow == null) {
                for (int i = leftGenericRow.getColumns().size();
                     i < joinSchema.fields().size(); i++) {
                  columns.add(null);
                }
              } else {
                columns.addAll(rightGenericRow.getColumns());
              }

              return new GenericRow(columns);
            },
            Joined.with(Serdes.String(),
                        joinSerDe.getGenericRowSerde(this.getSchema(),
                                                     ksqlConfig, false, schemaRegistryClient
                        ), null
            )
        );

    return new SchemaKStream(
        joinSchema,
        joinedKStream,
        joinKey,
        Arrays.asList(this, schemaKTable),
        Type.JOIN,
        functionRegistry,
        schemaRegistryClient
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream selectKey(final Field newKeyField, boolean updateRowKey) {
    if (keyField != null && keyField.name().equals(newKeyField.name())) {
      return this;
    }

    KStream keyedKStream = kstream.filter((key, value) ->
        value != null
            && extractColumn(newKeyField, value) != null
    ).selectKey((key, value) ->
        extractColumn(newKeyField, value)
            .toString()
    ).mapValues((key, row) -> {
      if (updateRowKey) {
        row.getColumns().set(SchemaUtil.ROWKEY_NAME_INDEX, key);
      }
      return row;
    });

    return new SchemaKStream(
        schema,
        keyedKStream,
        newKeyField,
        Collections.singletonList(this),
        Type.REKEY,
        functionRegistry,
        schemaRegistryClient
    );
  }

  private Object extractColumn(Field newKeyField, GenericRow value) {
    return value
        .getColumns()
        .get(SchemaUtil.getFieldIndexByName(schema, newKeyField.name()));
  }

  private String fieldNameFromExpression(Expression expression) {
    if (expression instanceof DereferenceExpression) {
      DereferenceExpression dereferenceExpression =
          (DereferenceExpression) expression;
      return dereferenceExpression.getFieldName();
    } else if (expression instanceof QualifiedNameReference) {
      QualifiedNameReference qualifiedNameReference = (QualifiedNameReference) expression;
      return qualifiedNameReference.getName().toString();
    }
    return null;
  }

  private boolean rekeyRequired(List<Expression> groupByExpressions) {
    Field keyField = getKeyField();
    if (keyField == null) {
      return true;
    }
    String keyFieldName = SchemaUtil.getFieldNameWithNoAlias(keyField);
    return !(groupByExpressions.size() == 1
        && fieldNameFromExpression(groupByExpressions.get(0)).equals(keyFieldName));
  }

  static String keyNameForGroupBy(final List<Expression> groupByExpressions) {
    return groupByExpressions.stream()
        .map(Expression::toString)
        .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR));
  }

  static List<Integer> keyIndexesForGroupBy(
      final Schema schema, final List<Expression> groupByExpressions) {
    return groupByExpressions.stream()
        .map(e -> SchemaUtil.getIndexInSchema(e.toString(), schema))
        .collect(Collectors.toList());
  }

  static String buildGroupByKey(final List<Integer> newKeyIndexes, final GenericRow value) {
    return newKeyIndexes.stream()
        .map(idx -> String.valueOf(value.getColumns().get(idx)))
        .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR));
  }

  public SchemaKGroupedStream groupBy(
      final Serde<String> keySerde,
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions) {
    boolean rekey = rekeyRequired(groupByExpressions);

    if (!rekey) {
      KGroupedStream kgroupedStream = kstream.groupByKey(Serialized.with(keySerde, valSerde));
      return new SchemaKGroupedStream(
          schema,
          kgroupedStream,
          keyField,
          Collections.singletonList(this),
          functionRegistry,
          schemaRegistryClient
      );
    }

    final String aggregateKeyName = keyNameForGroupBy(groupByExpressions);
    final List<Integer> newKeyIndexes = keyIndexesForGroupBy(getSchema(), groupByExpressions);

    KGroupedStream kgroupedStream = kstream.filter((key, value) -> value != null).groupBy(
        (key, value) -> buildGroupByKey(newKeyIndexes, value),
        Serialized.with(keySerde, valSerde));

    // TODO: if the key is a prefix of the grouping columns then we can
    //       use the repartition reflection hack to tell streams not to
    //       repartition.
    Field newKeyField = new Field(aggregateKeyName, -1, Schema.STRING_SCHEMA);
    return new SchemaKGroupedStream(
        schema,
        kgroupedStream,
        newKeyField,
        Collections.singletonList(this),
        functionRegistry,
        schemaRegistryClient);
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
    stringBuilder.append(indent)
        .append(" > [ ")
        .append(type).append(" ] Schema: ")
        .append(SchemaUtil.getSchemaDefinitionString(schema))
        .append(".\n");
    for (SchemaKStream schemaKStream : sourceSchemaKStreams) {
      stringBuilder
          .append("\t")
          .append(indent)
          .append(schemaKStream.getExecutionPlan(indent + "\t"));
    }
    return stringBuilder.toString();
  }

  public OutputNode outputNode() {
    return output;
  }

  public void setOutputNode(final OutputNode output) {
    this.output = output;
  }

  public Type getType() {
    return type;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistryClient;
  }
}
