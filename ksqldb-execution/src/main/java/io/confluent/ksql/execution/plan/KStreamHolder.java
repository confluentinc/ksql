/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import org.apache.kafka.streams.kstream.KStream;

@Immutable
public final class KStreamHolder<K> {

  private final KStream<K, GenericRow> stream;
  private final ExecutionKeyFactory<K> executionKeyFactory;
  private final LogicalSchema schema;

  public KStreamHolder(
      final KStream<K, GenericRow> stream,
      final LogicalSchema schema,
      final ExecutionKeyFactory<K> executionKeyFactory
  ) {
    this.stream = Objects.requireNonNull(stream, "stream");
    this.executionKeyFactory = Objects.requireNonNull(executionKeyFactory, "keySerdeFactory");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public ExecutionKeyFactory<K> getExecutionKeyFactory() {
    return executionKeyFactory;
  }

  public KStream<K, GenericRow> getStream() {
    return stream;
  }

  /**
   * Create a new {@code KStreamHolder} with the same {@code KStream} but a different schema.
   * @param stream The stream to use
   * @param schema The schema to use
   * @return The new {@code KStreamHolder}
   */
  public KStreamHolder<K> withStream(
      final KStream<K, GenericRow> stream,
      final LogicalSchema schema) {
    return new KStreamHolder<>(stream, schema, executionKeyFactory);
  }

  public LogicalSchema getSchema() {
    return schema;
  }
}
