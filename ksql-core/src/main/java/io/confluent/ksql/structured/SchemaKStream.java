/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.structured;

import io.confluent.ksql.function.udf.KUDF;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KSQLTopicSerDe;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class SchemaKStream {

  final Schema schema;
  final KStream kStream;
  final Field keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;

  public SchemaKStream(final Schema schema, final KStream kStream, final Field keyField,
                       final List<SchemaKStream> sourceSchemaKStreams) {
    this.schema = schema;
    this.kStream = kStream;
//    .map(new KeyValueMapper<String, GenericRow, KeyValue<String,
//        GenericRow>>() {
//      @Override
//      public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
//        row.getColumns().set(0, key);
//        return new KeyValue<>(key, row);
//      }
//    });
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
  }

  public QueuedSchemaKStream toQueue() {
    SynchronousQueue<KeyValue<String, GenericRow>> rowQueue = new SynchronousQueue<>();
    kStream.foreach(new QueuePopulator(rowQueue));
    return new QueuedSchemaKStream(this, rowQueue);
  }

  public SchemaKStream into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe,
      Set<Integer> rowkeyIndexes) {
    kStream
        .map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
      @Override
      public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
        List columns = new ArrayList();
        for (int i = 0; i < row.columns.size(); i++) {
          if (!rowkeyIndexes.contains(i)) {
            columns.add(row.columns.get(i));
          }
        }
        return new KeyValue<>(key, new GenericRow(columns));
      }
    }).to
        (Serdes.String(), topicValueSerDe, kafkaTopicName);
    return this;
  }

  public SchemaKStream filter(final Expression filterExpression) throws Exception {
    SQLPredicate predicate = new SQLPredicate(filterExpression, schema, false);
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
      schemaBuilder.field(expression.toString(), expressionEvaluator.getExpressionType());
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
                                KSQLTopicSerDe joinSerDe) {
//                                final Serde<GenericRow> resultValueSerDe) {

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
//    }, Serdes.String(), resultValueSerDe);
    }, Serdes.String(), SerDeUtil.getRowSerDe(joinSerDe, this.getSchema()));

    return new SchemaKStream(joinSchema, joinedKStream, joinKey, Arrays.asList(this, schemaKTable));
  }

  public SchemaKStream selectKey(final Field newKeyField) {
    if (keyField.name().equals(newKeyField.name())) {
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
    }).map(new KeyValueMapper<String, GenericRow, KeyValue<String,
        GenericRow>>() {
      @Override
      public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
        row.getColumns().set(0, key);
        return new KeyValue<>(key, row);
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

  protected static class QueuePopulator<K> implements ForeachAction<K, GenericRow> {
    private final SynchronousQueue<KeyValue<String, GenericRow>> queue;

    public QueuePopulator(SynchronousQueue<KeyValue<String, GenericRow>> queue) {
      this.queue = queue;
    }

    @Override
    public void apply(K key, GenericRow value) {
      try {
        String keyString;
        if (key instanceof Windowed) {
          Windowed windowedKey = (Windowed) key;
          keyString = String.format("%s : %s", windowedKey.key(), windowedKey.window());
        } else {
          keyString = Objects.toString(key);
        }
        queue.put(new KeyValue<>(keyString, value));
      } catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }
    }
  }
}
