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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

public class QueuedSchemaKStream extends SchemaKStream {

  private final BlockingQueue<KeyValue<String, GenericRow>> rowQueue =
      new LinkedBlockingQueue<>(100);

  private QueuedSchemaKStream(
      final Schema schema,
      final KStream kstream,
      final Field keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final FunctionRegistry functionRegistry,
      final Optional<Integer> limit,
      final OutputNode outputNode,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    super(
        schema,
        kstream,
        keyField,
        sourceSchemaKStreams,
        type,
        functionRegistry,
        schemaRegistryClient
    );
    setOutputNode(outputNode);
    kstream.foreach(new QueuedSchemaKStream.QueuePopulator(rowQueue, limit));
  }

  QueuedSchemaKStream(
      SchemaKStream schemaKStream,
      Optional<Integer> limit
  ) {
    this(
        schemaKStream.schema,
        schemaKStream.getKstream(),
        schemaKStream.keyField,
        schemaKStream.sourceSchemaKStreams,
        Type.SINK,
        schemaKStream.functionRegistry,
        limit,
        schemaKStream.outputNode(),
        schemaKStream.schemaRegistryClient
    );
  }

  public BlockingQueue<KeyValue<String, GenericRow>> getQueue() {
    return rowQueue;
  }

  @Override
  public SchemaKStream into(
      String kafkaTopicName,
      Serde<GenericRow> topicValueSerDe,
      Set<Integer> rowkeyIndexes
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream filter(Expression filterExpression) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream select(Schema selectSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream select(List<Pair<String, Expression>> expressions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream leftJoin(
      SchemaKTable schemaKTable,
      Schema joinSchema,
      Field joinKey,
      KsqlTopicSerDe joinSerDe,
      KsqlConfig ksqlConfig
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream selectKey(Field newKeyField, boolean updateRowKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKGroupedStream groupBy(
      final Serde<String> keySerde, final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions) {
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

  protected static class QueuePopulator<K> implements ForeachAction<K, GenericRow> {

    private final BlockingQueue<KeyValue<String, GenericRow>> queue;
    private final Optional<Integer> limit;
    private int counter = 0;

    QueuePopulator(
        BlockingQueue<KeyValue<String, GenericRow>> queue,
        Optional<Integer> limit
    ) {
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
          counter++;
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
        throw new KsqlException("InterruptedException while enqueueing:" + key);
      }
    }
  }
}
