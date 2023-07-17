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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
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
  ExecutionKeyFactory<K> withQueryBuilder(RuntimeBuildContext buildContext);

  /**
   * This method can construct a new key given the old key and the
   * desired contents of the new key. This is helpful if we intended
   * to maintain information from the previous key (e.g. the windowing information)
   * when constructing a new key.
   *
   * @return a new key of type {@code K} given the struct contents of the new key
   */
  K constructNewKey(K oldKey, GenericKey newKey);

  static ExecutionKeyFactory<GenericKey> unwindowed(final RuntimeBuildContext buildContext) {
    return new UnwindowedFactory(buildContext);
  }

  static ExecutionKeyFactory<Windowed<GenericKey>> windowed(
      final RuntimeBuildContext buildContext,
      final WindowInfo windowInfo
  ) {
    return new WindowedFactory(windowInfo, buildContext);
  }

  class UnwindowedFactory implements ExecutionKeyFactory<GenericKey> {

    private final RuntimeBuildContext buildContext;

    public UnwindowedFactory(final RuntimeBuildContext buildContext) {
      this.buildContext = Objects.requireNonNull(buildContext, "buildContext");
    }

    @Override
    public Serde<GenericKey> buildKeySerde(
        final FormatInfo format,
        final PhysicalSchema physicalSchema,
        final QueryContext queryContext
    ) {
      return buildContext.buildKeySerde(format, physicalSchema, queryContext);
    }

    @Override
    public ExecutionKeyFactory<GenericKey> withQueryBuilder(
        final RuntimeBuildContext buildContext
    ) {
      return new UnwindowedFactory(buildContext);
    }

    @Override
    public GenericKey constructNewKey(final GenericKey oldKey, final GenericKey newKey) {
      return newKey;
    }
  }

  class WindowedFactory implements ExecutionKeyFactory<Windowed<GenericKey>> {

    private final WindowInfo windowInfo;
    private final RuntimeBuildContext buildContext;

    public WindowedFactory(
        final WindowInfo windowInfo,
        final RuntimeBuildContext buildContext
    ) {
      this.windowInfo = Objects.requireNonNull(windowInfo, "windowInfo");
      this.buildContext = Objects.requireNonNull(buildContext, "buildContext");
    }

    @Override
    public Serde<Windowed<GenericKey>> buildKeySerde(
        final FormatInfo format,
        final PhysicalSchema physicalSchema,
        final QueryContext queryContext) {
      return buildContext.buildKeySerde(format, windowInfo, physicalSchema, queryContext);
    }

    @Override
    public ExecutionKeyFactory<Windowed<GenericKey>> withQueryBuilder(
        final RuntimeBuildContext buildContext
    ) {
      return new WindowedFactory(windowInfo, buildContext);
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
