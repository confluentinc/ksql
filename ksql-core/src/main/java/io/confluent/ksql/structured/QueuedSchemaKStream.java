/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class QueuedSchemaKStream extends SchemaKStream {

  private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;

  public QueuedSchemaKStream(final Schema schema, final KStream kstream, final Field keyField,
                             final List<SchemaKStream> sourceSchemaKStreams,
                             SynchronousQueue<KeyValue<String, GenericRow>> rowQueue,
                             Type type) {
    super(schema, kstream, keyField, sourceSchemaKStreams, type);
    this.rowQueue = rowQueue;
  }

  public QueuedSchemaKStream(SchemaKStream schemaKStream,
                             SynchronousQueue<KeyValue<String, GenericRow>> rowQueue,
                             Type type) {
    this(
        schemaKStream.schema,
        schemaKStream.kstream,
        schemaKStream.keyField,
        schemaKStream.sourceSchemaKStreams,
        rowQueue,
        type
    );
  }

  public SynchronousQueue<KeyValue<String, GenericRow>> getQueue() {
    return rowQueue;
  }

  @Override
  public SchemaKStream into(String kafkaTopicName, Serde<GenericRow> topicValueSerDe,
                            Set<Integer> rowkeyIndexes, KsqlConfig ksqlConfig, KafkaTopicClient kafkaTopicClient) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream filter(Expression filterExpression) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream select(Schema selectSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream select(List<Pair<String, Expression>> expressions) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream leftJoin(SchemaKTable schemaKTable, Schema joinSchema,
                                Field joinKey, KsqlTopicSerDe joinSerDe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream selectKey(Field newKeyField) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKGroupedStream groupByKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKGroupedStream groupByKey(Serde keySerde, Serde valSerde) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Field getKeyField() {
    return super.getKeyField();
  }

  @Override
  public Schema getSchema() {
    return super.getSchema();
  }

  @Override
  public KStream getKstream() {
    return super.getKstream();
  }

  @Override
  public List<SchemaKStream> getSourceSchemaKStreams() {
    return super.getSourceSchemaKStreams();
  }
}
