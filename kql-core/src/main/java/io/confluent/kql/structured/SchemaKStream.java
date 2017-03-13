/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.structured;

import io.confluent.kql.function.udf.KUDF;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.ExpressionMetadata;
import io.confluent.kql.util.ExpressionUtil;
import io.confluent.kql.util.GenericRowValueTypeEnforcer;
import io.confluent.kql.util.SchemaUtil;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class SchemaKStream {

  final Schema schema;
  final KStream kStream;
  final Field keyField;
  final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  final List<SchemaKStream> sourceSchemaKStreams;

  public SchemaKStream(final Schema schema, final KStream kStream, final Field keyField,
                       final List<SchemaKStream> sourceSchemaKStreams) {
    this.schema = schema;
    this.kStream = kStream;
    this.keyField = keyField;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.sourceSchemaKStreams = sourceSchemaKStreams;
  }

  public SchemaKStream into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe) {

    kStream.to(Serdes.String(), topicValueSerDe, kafkaTopicName);
    return this;
  }

  public SchemaKStream print() {
    KStream
        printKStream =
        kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow genericRow) {
            System.out.println(key + " ==> " + genericRow.toString());
            return new KeyValue<String, GenericRow>(key, genericRow);
          }

        });
    return this;
  }

  public SchemaKStream filter(final Expression filterExpression) throws Exception {
    SQLPredicate predicate = new SQLPredicate(filterExpression, schema);
    KStream filteredKStream = kStream.filter(predicate.getPredicate());
    return new SchemaKStream(schema, filteredKStream, keyField, Arrays.asList(this));
  }

  public SchemaKStream select(final Schema selectSchema) {

    KStream
        projectedKStream =
        kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
            List<Object> newColumns = new ArrayList();
            for (Field schemaField : selectSchema.fields()) {
              newColumns.add(
                  row.getColumns().get(SchemaUtil.getFieldIndexByName(schema, schemaField.name())));
            }
            GenericRow newRow = new GenericRow(newColumns);
            return new KeyValue<String, GenericRow>(key, newRow);
          }
        });

    return new SchemaKStream(selectSchema, projectedKStream, keyField, Arrays.asList(this));
  }

  public SchemaKStream select(final List<Expression> expressions)
      throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    // TODO: Optimize to remove the code gen for constants and single columns references and use them directly.
    // TODO: Only use code get when we have real expression.
    List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Expression expression : expressions) {
      ExpressionMetadata
          expressionEvaluator =
          expressionUtil.getExpressionEvaluator(expression, schema);
      schemaBuilder.field(expression.toString(), SchemaUtil.getTypeSchema(expressionEvaluator.getExpressionType()));
      expressionEvaluators.add(expressionEvaluator);
    }
    KStream
        projectedKStream =
        kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
            List<Object> newColumns = new ArrayList();
            for (int i = 0; i < expressions.size(); i++) {
              Expression expression = expressions.get(i);
              int[] parameterIndexes = expressionEvaluators.get(i).getIndexes();
              KUDF[] kudfs = expressionEvaluators.get(i).getUdfs();
              Object[] parameterObjects = new Object[parameterIndexes.length];
              for (int j = 0; j < parameterIndexes.length; j++) {
                if (parameterIndexes[j] < 0) {
                  parameterObjects[j] = kudfs[j];
                } else {
                  parameterObjects[j] = genericRowValueTypeEnforcer.enforceFieldType(parameterIndexes[j], row.getColumns().get(parameterIndexes[j]));
                }
              }
              Object columnValue = null;
              try {
                columnValue = expressionEvaluators.get(i).getExpressionEvaluator().evaluate(parameterObjects);
              } catch (InvocationTargetException e) {
                e.printStackTrace();
              }
              newColumns.add(columnValue);

            }
            GenericRow newRow = new GenericRow(newColumns);
            return new KeyValue<String, GenericRow>(key, newRow);
          }
        });

    return new SchemaKStream(schemaBuilder.build(), projectedKStream, keyField, Arrays.asList(this));
  }

  public SchemaKStream leftJoin(final SchemaKTable schemaKTable, final Schema joinSchema,
                                final Field joinKey,
                                final Serde<GenericRow> resultValueSerDe) {

    KStream joinedKStream = kStream.leftJoin(schemaKTable.getkTable(), new ValueJoiner<GenericRow, GenericRow, GenericRow>() {
      @Override
      public GenericRow apply(GenericRow leftGenericRow, GenericRow rightGenericRow) {
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
      }
    }, Serdes.String(), resultValueSerDe);

    return new SchemaKStream(joinSchema, joinedKStream, joinKey, Arrays.asList(this, schemaKTable));
  }

  public SchemaKStream selectKey(final Field newKeyField) {
    if (keyField.name().equalsIgnoreCase(newKeyField.name())) {
      return this;
    }

    KStream keyedKStream = kStream.selectKey(new KeyValueMapper<String, GenericRow, String>() {
      @Override
      public String apply(String key, GenericRow value) {

        String
            newKey =
            value.getColumns().get(SchemaUtil.getFieldIndexByName(schema, newKeyField.name()))
                .toString();
        return newKey;
      }
    });

    return new SchemaKStream(schema, keyedKStream, newKeyField, Arrays.asList(this));
  }

  public SchemaKGroupedStream groupByKey() {
    KGroupedStream kGroupedStream = kStream.groupByKey();
    return new SchemaKGroupedStream(schema, kGroupedStream, keyField, Arrays.asList(this));
  }

  public SchemaKGroupedStream groupByKey(final Serde keySerde,
                                         final Serde valSerde) {
    KGroupedStream kGroupedStream = kStream.groupByKey(keySerde, valSerde);
    return new SchemaKGroupedStream(schema, kGroupedStream, keyField, Arrays.asList(this));
  }

  public Field getKeyField() {
    return keyField;
  }

  public Schema getSchema() {
    return schema;
  }

  public KStream getkStream() {
    return kStream;
  }

  public List<SchemaKStream> getSourceSchemaKStreams() {
    return sourceSchemaKStreams;
  }
}
