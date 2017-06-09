/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;

import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class QueuedSchemaKStream extends SchemaKStream {

  private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;

  public QueuedSchemaKStream(final Schema schema, final KStream kStream, final Field keyField,
                             final List<SchemaKStream> sourceSchemaKStreams,
                             SynchronousQueue<KeyValue<String, GenericRow>> rowQueue) {
    super(schema, kStream, keyField, sourceSchemaKStreams);
    this.rowQueue = rowQueue;
  }

  public QueuedSchemaKStream(SchemaKStream schemaKStream,
                             SynchronousQueue<KeyValue<String, GenericRow>> rowQueue) {
    this(
        schemaKStream.schema,
        schemaKStream.kStream,
        schemaKStream.keyField,
        schemaKStream.sourceSchemaKStreams,
        rowQueue
    );
  }

  public SynchronousQueue<KeyValue<String, GenericRow>> getQueue() {
    return rowQueue;
  }

  @Override
  public SchemaKStream into(String kafkaTopicName, Serde<GenericRow> topicValueSerDe,
                            Set<Integer> rowkeyIndexes, final StreamsKafkaClient
                                  streamsKafkaClient, KsqlConfig ksqlConfig) {
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
  public SchemaKStream select(List<Expression> expressions) throws Exception {
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
  public KStream getkStream() {
    return super.getkStream();
  }

  @Override
  public List<SchemaKStream> getSourceSchemaKStreams() {
    return super.getSourceSchemaKStreams();
  }
}
