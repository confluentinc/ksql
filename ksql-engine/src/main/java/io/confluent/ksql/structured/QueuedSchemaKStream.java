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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;

public class QueuedSchemaKStream<K> extends SchemaKStream<K> {
  public QueuedSchemaKStream(final SchemaKStream<K> schemaKStream) {
    super(
        schemaKStream.getKstream(),
        schemaKStream.getSourceStep(),
        schemaKStream.getSourceProperties(),
        schemaKStream.keyFormat,
        schemaKStream.keySerde,
        schemaKStream.keyField,
        schemaKStream.sourceSchemaKStreams,
        Type.SINK,
        schemaKStream.ksqlConfig,
        schemaKStream.functionRegistry,
        schemaKStream.streamsFactories
    );
  }

  @Override
  public SchemaKStream<K> into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final LogicalSchema outputSchema,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final Set<Integer> rowkeyIndexes,
      final QueryContext.Stacker contextStacker
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
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final ValueFormat valueFormat,
      final Serde<GenericRow> joinSerde,
      final QueryContext.Stacker contextStacker
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKStream<Struct> selectKey(
      final String fieldName,
      final boolean updateRowKey,
      final QueryContext.Stacker contextStacker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaKGroupedStream groupBy(
      final ValueFormat valueFormat,
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker) {
    throw new UnsupportedOperationException();
  }
}
