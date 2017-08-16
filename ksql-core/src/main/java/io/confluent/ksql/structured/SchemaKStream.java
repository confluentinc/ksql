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
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;
import io.confluent.ksql.util.KafkaTopicClient;

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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class SchemaKStream {
  
  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN, TOSTREAM }

  protected final Schema schema;
  protected final KStream kstream;
  protected final Field keyField;
  protected final List<SchemaKStream> sourceSchemaKStreams;
  protected final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  protected final Type type;

  private static final Logger log = LoggerFactory.getLogger(SchemaKStream.class);

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
    SynchronousQueue<KeyValue<String, GenericRow>> rowQueue = new SynchronousQueue<>();
    kstream.foreach(new QueuePopulator(rowQueue, limit));
    return new QueuedSchemaKStream(this, rowQueue, Type.SINK);
  }

  public SchemaKStream into(final String kafkaTopicName, final Serde<GenericRow> topicValueSerDe,
                            final Set<Integer> rowkeyIndexes, KsqlConfig ksqlConfig, KafkaTopicClient kafkaTopicClient) {

    createSinkTopic(kafkaTopicName, ksqlConfig, kafkaTopicClient);

    kstream
        .map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
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
          }
        }).to(Serdes.String(), topicValueSerDe, kafkaTopicName);
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

    return new SchemaKStream(selectSchema, projectedKStream, keyField, Arrays.asList(this),
                             Type.PROJECT);
  }

  public SchemaKStream select(final List<Pair<String, Expression>> expressionPairList)
      throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    // TODO: Optimize to remove the code gen for constants and single columns references
    // TODO: and use them directly.
    // TODO: Only use code get when we have real expression.
    List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Pair<String, Expression> expressionPair : expressionPairList) {
      ExpressionMetadata
          expressionEvaluator =
          expressionUtil.getExpressionEvaluator(expressionPair.getRight(), schema);
      schemaBuilder.field(expressionPair.getLeft(), expressionEvaluator.getExpressionType());
      expressionEvaluators.add(expressionEvaluator);
    }
    KStream
        projectedKStream =
        kstream.mapValues(new ValueMapper<GenericRow, GenericRow>() {
          @Override
          public GenericRow apply(GenericRow row) {
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
                      parameterObjects[j] = genericRowValueTypeEnforcer
                          .enforceFieldType(parameterIndexes[j],
                                            row.getColumns().get(parameterIndexes[j]));
                    }
                  }
                  Object columnValue = null;
                  columnValue = expressionEvaluators
                      .get(i).getExpressionEvaluator().evaluate(parameterObjects);
                  newColumns.add(columnValue);
                } catch (Exception ex) {
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
          }
        });

    return new SchemaKStream(schemaBuilder.build(),
                             projectedKStream, keyField, Arrays.asList(this),
                             Type.PROJECT);
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
                             Arrays.asList(this, schemaKTable), Type.JOIN);
  }

  public SchemaKStream selectKey(final Field newKeyField) {
    if (keyField != null &&
        keyField.name().equals(newKeyField.name())) {
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
        row.getColumns().set(SchemaUtil.ROWKEY_NAME_INDEX, key);
        return new KeyValue<>(key, row);
      }
    });

    return new SchemaKStream(schema, keyedKStream, newKeyField, Arrays.asList(this),
                             Type.REKEY);
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
    private final Optional<Integer> limit;
    private int counter = 0;

    public QueuePopulator(SynchronousQueue<KeyValue<String, GenericRow>> queue,
                          Optional<Integer> limit) {
      this.queue = queue;
      this.limit = limit;
    }

    @Override
    public void apply(K key, GenericRow row) {
      try {
        if (row == null) {
          return;
        }
        if (limit.isPresent()) {
          counter ++;
          if (counter > limit.get()) {
            throw new KsqlException("LIMIT reached for the partition.");
          }
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

  public String getExecutionPlan(String indent) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(indent + " > [ " + type + " ] Schema: " + SchemaUtil
        .getSchemaDefinitionString(schema) + ".\n");
    for (SchemaKStream schemaKStream: sourceSchemaKStreams) {
      stringBuilder.append("\t" + indent + schemaKStream.getExecutionPlan(indent + "\t"));
    }
    return stringBuilder.toString();
  }

  protected void createSinkTopic(final String kafkaTopicName, KsqlConfig ksqlConfig, KafkaTopicClient kafkaTopicClient) {
    int numberOfPartitions = (Integer) ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS);
    short numberOfReplications = (Short) ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS);
    kafkaTopicClient.createTopic(kafkaTopicName, numberOfPartitions, numberOfReplications);
  }
}
