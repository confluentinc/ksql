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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * The {@code ExecutionKeyFactory} is in charge of creating the keys
 * and the Serdes for the keys of any particular execution step.
 *
 * @param <K> the type of the key, usually either {@code GenericKey}
 *            or {@code Windowed<GenericKey>}
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
   * This method can construct a new key given the old key and the
   * desired contents of the new key. This is helpful if we intended
   * to maintain information from the previous key (e.g. the windowing information)
   * when constructing a new key.
   *
   * @return a new key of type {@code K} given the struct contents of the new key
   */
  K constructNewKey(K oldKey, GenericKey newKey);

  static ExecutionKeyFactory<GenericKey> unwindowed(final KsqlQueryBuilder queryBuilder) {
    return new UnwindowedFactory(queryBuilder);
  }

  static ExecutionKeyFactory<Windowed<GenericKey>> windowed(
      final KsqlQueryBuilder queryBuilder,
      final WindowInfo windowInfo
  ) {
    return new WindowedFactory(windowInfo, queryBuilder);
  }

  class UnwindowedFactory implements ExecutionKeyFactory<GenericKey> {

    private final KsqlQueryBuilder queryBuilder;

    public UnwindowedFactory(final KsqlQueryBuilder queryBuilder) {
      this.queryBuilder = Objects.requireNonNull(queryBuilder, "queryBuilder");
    }

    @Override
    public Serde<GenericKey> buildKeySerde(
        final FormatInfo format,
        final PhysicalSchema physicalSchema,
        final QueryContext queryContext
    ) {
      return queryBuilder.buildKeySerde(format, physicalSchema, queryContext);
    }

    @Override
    public ExecutionKeyFactory<GenericKey> withQueryBuilder(final KsqlQueryBuilder builder) {
      return new UnwindowedFactory(builder);
    }

    @Override
    public GenericKey constructNewKey(final GenericKey oldKey, final GenericKey newKey) {
      return newKey;
    }
  }

  class WindowedFactory implements ExecutionKeyFactory<Windowed<GenericKey>> {

    private final WindowInfo windowInfo;
    private final KsqlQueryBuilder queryBuilder;

    public WindowedFactory(final WindowInfo windowInfo, final KsqlQueryBuilder queryBuilder) {
      this.windowInfo = Objects.requireNonNull(windowInfo, "windowInfo");
      this.queryBuilder = Objects.requireNonNull(queryBuilder, "queryBuilder");
    }

    @Override
    public Serde<Windowed<GenericKey>> buildKeySerde(
        final FormatInfo format,
        final PhysicalSchema physicalSchema,
        final QueryContext queryContext) {
      return queryBuilder.buildKeySerde(format, windowInfo, physicalSchema, queryContext);
    }

    @Override
    public ExecutionKeyFactory<Windowed<GenericKey>> withQueryBuilder(
        final KsqlQueryBuilder builder
    ) {
      return new WindowedFactory(windowInfo, builder);
    }

    @Override
    public Windowed<GenericKey> constructNewKey(
        final Windowed<GenericKey> oldKey,
        final GenericKey newKey
    ) {
      return new Windowed<>(newKey, oldKey.window());
    }
  }
}
