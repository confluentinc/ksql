/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.structured;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
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
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class SchemaKStream {

  final Schema schema;
  final KStream kstream;
  final Field keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;

  private static final Logger log = LoggerFactory.getLogger(SchemaKStream.class);

  public SchemaKStream(final Schema schema, final KStream kstream, final Field keyField,
                       final List<SchemaKStream> sourceSchemaKStreams) {
    this.schema = schema;
    this.kstream = kstream;
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
  }

  public QueuedSchemaKStream toQueue() {
    SynchronousQueue<KeyValue<String, GenericRow>> rowQueue = new SynchronousQueue<>();
    kstream.foreach(new QueuePopulator(rowQueue));
    return new QueuedSchemaKStream(this, rowQueue);
  }

  public SchemaKStream into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe,
                            final Set<Integer> rowkeyIndexes, final StreamsKafkaClient
                            streamsKafkaClient, KsqlConfig ksqlConfig) {

    createSinkTopic(kafkaTopicName, streamsKafkaClient, ksqlConfig);

    kstream
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
        }).to(Serdes.String(), topicValueSerDe, kafkaTopicName);
    return this;
  }

  public SchemaKStream filter(final Expression filterExpression) throws Exception {
    SqlPredicate predicate = new SqlPredicate(filterExpression, schema, false);
    KStream filteredKStream = kstream.filter(predicate.getPredicate());
    return new SchemaKStream(schema, filteredKStream, keyField, Arrays.asList(this));
  }

  public SchemaKStream select(final Schema selectSchema) {

    KStream
        projectedKStream =
        kstream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
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
    // TODO: Optimize to remove the code gen for constants and single columns references
    // TODO: and use them directly.
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
        kstream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
            List<Object> newColumns = new ArrayList();
            for (int i = 0; i < expressions.size(); i++) {
              Expression expression = expressions.get(i);
              int[] parameterIndexes = expressionEvaluators.get(i).getIndexes();
              Kudf[] kudfs = expressionEvaluators.get(i).getUdfs();
              Object[] parameterObjects = new Object[parameterIndexes.length];
              for (int j = 0; j < parameterIndexes.length; j++) {
                if (parameterIndexes[j] < 0) {
                  parameterObjects[j] = kudfs[j];
                } else {
                  parameterObjects[j] = genericRowValueTypeEnforcer
                      .enforceFieldType(parameterIndexes[j],
                                        row.getColumns().get(parameterIndexes[j]));
                }
              }
              Object columnValue = null;
              try {
                columnValue = expressionEvaluators
                    .get(i).getExpressionEvaluator().evaluate(parameterObjects);
              } catch (InvocationTargetException e) {
                e.printStackTrace();
              }
              newColumns.add(columnValue);

            }
            GenericRow newRow = new GenericRow(newColumns);
            return new KeyValue<String, GenericRow>(key, newRow);
          }
        });

    return new SchemaKStream(schemaBuilder.build(),
                             projectedKStream, keyField, Arrays.asList(this));
  }

  public SchemaKStream leftJoin(final SchemaKTable schemaKTable, final Schema joinSchema,
                                final Field joinKey,
                                KsqlTopicSerDe joinSerDe) {

    KStream joinedKStream =
        kstream.leftJoin(
            schemaKTable.getKtable(), new ValueJoiner<GenericRow, GenericRow, GenericRow>() {
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
    }, Serdes.String(), SerDeUtil.getRowSerDe(joinSerDe, this.getSchema()));

    return new SchemaKStream(joinSchema, joinedKStream, joinKey,
                             Arrays.asList(this, schemaKTable));
  }

  public SchemaKStream selectKey(final Field newKeyField) {
    if (keyField.name().equals(newKeyField.name())) {
      return this;
    }

    KStream keyedKStream = kstream.selectKey(new KeyValueMapper<String, GenericRow, String>() {
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
    KGroupedStream kgroupedStream = kstream.groupByKey();
    return new SchemaKGroupedStream(schema, kgroupedStream, keyField, Arrays.asList(this));
  }

  public SchemaKGroupedStream groupByKey(final Serde keySerde,
                                         final Serde valSerde) {
    KGroupedStream kgroupedStream = kstream.groupByKey(keySerde, valSerde);
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

  protected static class QueuePopulator<K> implements ForeachAction<K, GenericRow> {
    private final SynchronousQueue<KeyValue<String, GenericRow>> queue;

    public QueuePopulator(SynchronousQueue<KeyValue<String, GenericRow>> queue) {
      this.queue = queue;
    }

    @Override
    public void apply(K key, GenericRow row) {
      try {
        if (row == null) {
          return;
        }
        String keyString;
        if (key instanceof Windowed) {
          Windowed windowedKey = (Windowed) key;
          keyString = String.format("%s : %s", windowedKey.key(), windowedKey.window());
        } else {
          keyString = Objects.toString(key);
        }
        queue.put(new KeyValue<>(keyString, row));
      } catch (InterruptedException exception) {
        log.error(" Exception while enqueuing the row: " + key + " : " + row);
        log.error(" Exception: " + exception.getMessage());
      }
    }
  }

  protected void createSinkTopic(
      final String kafkaTopicName,
      final StreamsKafkaClient streamsKafkaClient, KsqlConfig ksqlConfig) {
    InternalTopicConfig internalTopicConfig =
        new InternalTopicConfig(kafkaTopicName,
                                Utils.mkSet(InternalTopicConfig.CleanupPolicy
                                                                 .compact,
                                                             InternalTopicConfig.CleanupPolicy
                                                                 .delete),
                                Collections.<String, String>emptyMap());
    Map<InternalTopicConfig, Integer> topics = new HashMap<>();
    int numberOfPartitions = (Integer) ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS);
    short numberOfReplications = (Short) ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS);
    long windowChangeLogAdditionalRetention =
        (Long) ksqlConfig.get(KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION);
    topics.put(internalTopicConfig, numberOfPartitions);
    streamsKafkaClient.createTopics(topics, numberOfReplications,
                                    windowChangeLogAdditionalRetention,
                                    streamsKafkaClient.fetchMetadata());
  }
}
