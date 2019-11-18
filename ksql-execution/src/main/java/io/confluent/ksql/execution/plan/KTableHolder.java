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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.kstream.KTable;

public final class KTableHolder<K> {
  private final KTable<K, GenericRow> stream;
  private final KeySerdeFactory<K> keySerdeFactory;
  private final LogicalSchema schema;
  private final Optional<MaterializationInfo.Builder> materializationBuilder;

  private KTableHolder(
      final KTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final KeySerdeFactory<K> keySerdeFactory,
      final Optional<MaterializationInfo.Builder> materializationBuilder
  ) {
    this.stream = Objects.requireNonNull(stream, "stream");
    this.keySerdeFactory = Objects.requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.materializationBuilder =
        Objects.requireNonNull(materializationBuilder, "materializationProvider");
  }

  public static <K> KTableHolder<K> unmaterialized(
      final KTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final KeySerdeFactory<K> keySerdeFactory
  ) {
    return new KTableHolder<>(stream, schema, keySerdeFactory, Optional.empty());
  }

  public static <K> KTableHolder<K> materialized(
      final KTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final KeySerdeFactory<K> keySerdeFactory,
      final MaterializationInfo.Builder materializationBuilder
  ) {
    return new KTableHolder<>(stream, schema, keySerdeFactory, Optional.of(materializationBuilder));
  }

  public KeySerdeFactory<K> getKeySerdeFactory() {
    return keySerdeFactory;
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
    return new KTableHolder<>(table, schema, keySerdeFactory, materializationBuilder);
  }

  public KTableHolder<K> withMaterialization(final Optional<MaterializationInfo.Builder> builder) {
    return new KTableHolder<>(
        stream,
        schema,
        keySerdeFactory,
        builder
    );
  }
}