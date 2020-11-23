/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * The {@code ExecutionKeyFactory} is in charge of creating the keys
 * and the Serdes for the keys of any particular execution step.
 *
 * @param <K> the type of the key, usually either {@code Struct}
 *            or {@code Windowed<Struct>}
 */
@EffectivelyImmutable
public interface ExecutionKeyFactory<K> {

  Serde<K> buildKeySerde(
      FormatInfo format,
      PhysicalSchema physicalSchema,
      QueryContext queryContext
  );

  /**
   * @return a new {@code ExecutionKeyFactory}
   */
  ExecutionKeyFactory<K> withQueryBuilder(KsqlQueryBuilder builder);

  /**
   * This method can construct a new key given the contents of the old key and the
   * desired Struct representation of the new key. This is helpful if we intended
   * to maintain information from the previous key (e.g. the windowing information)
   * when constructing a new key.
   *
   * @return a new key of type {@code K} given the struct contents of the new key
   */
  K constructNewKey(K oldKey, Struct newKey);

  static ExecutionKeyFactory<Struct> unwindowed(final KsqlQueryBuilder queryBuilder) {
    return new UnwindowedFactory(queryBuilder);
  }

  static ExecutionKeyFactory<Windowed<Struct>> windowed(
      final KsqlQueryBuilder queryBuilder,
      final WindowInfo windowInfo
  ) {
    return new WindowedFactory(windowInfo, queryBuilder);
  }

  class UnwindowedFactory implements ExecutionKeyFactory<Struct> {

    private final KsqlQueryBuilder queryBuilder;

    public UnwindowedFactory(final KsqlQueryBuilder queryBuilder) {
      this.queryBuilder = Objects.requireNonNull(queryBuilder, "queryBuilder");
    }

    @Override
    public Serde<Struct> buildKeySerde(
        final FormatInfo format,
        final PhysicalSchema physicalSchema,
        final QueryContext queryContext
    ) {
      return queryBuilder.buildKeySerde(format, physicalSchema, queryContext);
    }

    @Override
    public ExecutionKeyFactory<Struct> withQueryBuilder(final KsqlQueryBuilder builder) {
      return new UnwindowedFactory(builder);
    }

    @Override
    public Struct constructNewKey(final Struct oldKey, final Struct newKey) {
      return newKey;
    }
  }

  class WindowedFactory implements ExecutionKeyFactory<Windowed<Struct>> {

    private final WindowInfo windowInfo;
    private final KsqlQueryBuilder queryBuilder;

    public WindowedFactory(final WindowInfo windowInfo, final KsqlQueryBuilder queryBuilder) {
      this.windowInfo = Objects.requireNonNull(windowInfo, "windowInfo");
      this.queryBuilder = Objects.requireNonNull(queryBuilder, "queryBuilder");
    }

    @Override
    public Serde<Windowed<Struct>> buildKeySerde(
        final FormatInfo format,
        final PhysicalSchema physicalSchema,
        final QueryContext queryContext) {
      return queryBuilder.buildKeySerde(format, windowInfo, physicalSchema, queryContext);
    }

    @Override
    public ExecutionKeyFactory<Windowed<Struct>> withQueryBuilder(final KsqlQueryBuilder builder) {
      return new WindowedFactory(windowInfo, builder);
    }

    @Override
    public Windowed<Struct> constructNewKey(final Windowed<Struct> oldKey, final Struct newKey) {
      return new Windowed<>(newKey, oldKey.window());
    }
  }
}
