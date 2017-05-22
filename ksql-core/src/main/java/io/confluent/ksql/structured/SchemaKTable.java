/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.structured;

import io.confluent.ksql.function.udf.KUDF;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class SchemaKTable extends SchemaKStream {

  final KTable kTable;
  final boolean isWindowed;

  public SchemaKTable(final Schema schema, final KTable kTable, final Field keyField,
                      final List<SchemaKStream> sourceSchemaKStreams, boolean isWindowed) {
    super(schema, null, keyField, sourceSchemaKStreams);
    this.kTable = kTable;
    this.isWindowed = isWindowed;
  }

  @Override
  public SchemaKTable into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe,
                           Set<Integer> rowkeyIndexes, final StreamsKafkaClient
                                 streamsKafkaClient, KSQLConfig ksqlConfig) {

    createSinkTopic(kafkaTopicName, streamsKafkaClient, ksqlConfig);

    if (isWindowed) {
      kTable.toStream().map(new KeyValueMapper<Windowed<String>, GenericRow, KeyValue<Windowed<String>, GenericRow>>() {
        @Override
        public KeyValue<Windowed<String>, GenericRow> apply(Windowed<String> key, GenericRow row) {
          if (row == null) {
            return new KeyValue<>(key, null);
          }
          List columns = new ArrayList();
          for (int i = 0; i < row.columns.size(); i++) {
            if (!rowkeyIndexes.contains(i)) {
              columns.add(row.columns.get(i));
            }
          }
          return new KeyValue<>(key, new GenericRow(columns));
        }
      }).to(new WindowedSerde(), topicValueSerDe, kafkaTopicName);
    } else {
      kTable.toStream().map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
        @Override
        public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
          if (row == null) {
            return new KeyValue<>(key, null);
          }
          List columns = new ArrayList();
          for (int i = 0; i < row.columns.size(); i++) {
            if (!rowkeyIndexes.contains(i)) {
              columns.add(row.columns.get(i));
            }
          }
          return new KeyValue<>(key, new GenericRow(columns));
        }
      }).to(Serdes.String(), topicValueSerDe, kafkaTopicName);
    }

    return this;
  }

  @Override
  public QueuedSchemaKStream toQueue() {
    SynchronousQueue<KeyValue<String, GenericRow>> rowQueue = new SynchronousQueue<>();
    kTable.toStream().foreach(new QueuePopulator(rowQueue));
    return new QueuedSchemaKStream(this, rowQueue);
  }

  @Override
  public SchemaKTable filter(final Expression filterExpression) throws Exception {
    SQLPredicate predicate = new SQLPredicate(filterExpression, schema, isWindowed);
    KTable filteredKTable = kTable.filter(predicate.getPredicate());
    return new SchemaKTable(schema, filteredKTable, keyField, Arrays.asList(this), isWindowed);
  }

  @Override
  public SchemaKTable select(final List<Expression> expressions)
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

    KTable projectedKTable = kTable.mapValues(new ValueMapper<GenericRow, GenericRow>() {
      @Override
      public GenericRow apply(GenericRow row) {
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
        return newRow;
      }
    });

    return new SchemaKTable(schemaBuilder.build(), projectedKTable, keyField, Arrays.asList(this), isWindowed);
  }

  public SchemaKStream toStream() {
    return new SchemaKStream(schema, kTable.toStream(), keyField, sourceSchemaKStreams);
  }

  public KTable getkTable() {
    return kTable;
  }

  public boolean isWindowed() {
    return isWindowed;
  }
}
