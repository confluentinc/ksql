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
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.kstream.KTable;

@Immutable
public final class KTableHolder<K> {

  private final KTable<K, GenericRow> stream;
  private final ExecutionKeyFactory<K> executionKeyFactory;
  private final LogicalSchema schema;
  @EffectivelyImmutable // Ignored
  private final Optional<MaterializationInfo.Builder> materializationBuilder;

  private KTableHolder(
      final KTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final ExecutionKeyFactory<K> executionKeyFactory,
      final Optional<MaterializationInfo.Builder> materializationBuilder
  ) {
    this.stream = Objects.requireNonNull(stream, "stream");
    this.executionKeyFactory = Objects.requireNonNull(executionKeyFactory, "keySerdeFactory");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.materializationBuilder =
        Objects.requireNonNull(materializationBuilder, "materializationProvider");
  }

  public static <K> KTableHolder<K> unmaterialized(
      final KTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final ExecutionKeyFactory<K> executionKeyFactory
  ) {
    return new KTableHolder<>(stream, schema, executionKeyFactory, Optional.empty());
  }

  public static <K> KTableHolder<K> materialized(
      final KTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final ExecutionKeyFactory<K> executionKeyFactory,
      final MaterializationInfo.Builder materializationBuilder
  ) {
    return new KTableHolder<>(
        stream,
        schema,
        executionKeyFactory,
        Optional.of(materializationBuilder)
    );
  }

  public ExecutionKeyFactory<K> getExecutionKeyFactory() {
    return executionKeyFactory;
  }

  public KTable<K, GenericRow> getTable() {
    return stream;
  }

  public Optional<MaterializationInfo.Builder> getMaterializationBuilder() {
    return materializationBuilder;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KTableHolder<K> withTable(final KTable<K, GenericRow> table, final LogicalSchema schema) {
    return new KTableHolder<>(table, schema, executionKeyFactory, materializationBuilder);
  }

  public KTableHolder<K> withMaterialization(final Optional<MaterializationInfo.Builder> builder) {
    return new KTableHolder<>(
        stream,
        schema,
        executionKeyFactory,
        builder
    );
  }
}