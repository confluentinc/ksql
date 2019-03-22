/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.SelectExpression;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KStream;

public class QueuedSchemaKStream<K> extends SchemaKStream<K> {

  public QueuedSchemaKStream(
      final SchemaKStream<K> schemaKStream,
      final QueryContext queryContext
  ) {
    super(
        schemaKStream.schema,
        schemaKStream.getKstream(),
        schemaKStream.keyField,
        schemaKStream.sourceSchemaKStreams,
        schemaKStream.keySerdeFactory,
        Type.SINK,
        schemaKStream.ksqlConfig,
        schemaKStream.functionRegistry,
        queryContext
    );
  }

  @Override
  public SchemaKStream<K> into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final Set<Integer> rowkeyIndexes
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream<K> select(
      final List<SelectExpression> expressions,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final Schema joinSchema,
      final Field joinKey,
      final Serde<GenericRow> joinSerde,
      final QueryContext.Stacker contextStacker
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream<K> selectKey(
      final Field newKeyField,
      final boolean updateRowKey,
      final QueryContext.Stacker contextStacker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKGroupedStream groupBy(
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker) {
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
  public KStream<K, GenericRow> getKstream() {
    return super.getKstream();
  }

  @Override
  public List<SchemaKStream> getSourceSchemaKStreams() {
    return super.getSourceSchemaKStreams();
  }
}
