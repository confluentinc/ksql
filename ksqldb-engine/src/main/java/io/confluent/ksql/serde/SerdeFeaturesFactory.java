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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public final class SerdeFeaturesFactory {

  private SerdeFeaturesFactory() {
  }

  /**
   * Build serde options for internal topics.
   *
   * <p>Internal topics don't normally need any serde features set, as they use the format
   * defaults.  However, until ksqlDB supports wrapped single keys, any internal topic with a key
   * format that supports both wrapping and unwrapping needs to have an explicit {@link
   * SerdeFeature#UNWRAP_SINGLES} set to ensure backwards compatibility is easily achievable once
   * wrapped keys are supported.
   *
   * <p>Note: The unwrap feature should only be set when there is only a single key column. As
   * ksql does not yet support multiple key columns, the only time there is no a single key column
   * is when there is no key column, i.e. key-less streams. Internal topics, i.e. changelog and
   * repartition topics, are never key-less. Hence this method can safely set the unwrap feature
   * without checking the schema.
   *
   * <p>The code that sets the feature can be removed once wrapped keys are supported. Issue 6296
   * tracks the removal.
   *
   * @param keyFormat the key format.
   * @return the options
   * @see <a href=https://github.com/confluentinc/ksql/issues/6296>Issue 6296</a>
   * @see InternalFormats#of
   */
  public static SerdeFeatures buildInternal(final Format keyFormat) {
    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    getKeyWrapping(true, keyFormat)
        .ifPresent(builder::add);

    return SerdeFeatures.from(builder.build());
  }

  public static SerdeFeatures buildKeyFeatures(
      final LogicalSchema schema,
      final Format keyFormat
  ) {
    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    getKeyWrapping(schema.key().size() == 1, keyFormat)
        .ifPresent(builder::add);

    return SerdeFeatures.from(builder.build());
  }

  public static SerdeFeatures buildValueFeatures(
      final LogicalSchema schema,
      final Format valueFormat,
      final SerdeFeatures explicitFeatures,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleColumn = schema.value().size() == 1;

    final ImmutableSet.Builder<SerdeFeature> builder = ImmutableSet.builder();

    getValueWrapping(singleColumn, valueFormat, explicitFeatures, ksqlConfig)
        .ifPresent(builder::add);

    return SerdeFeatures.from(builder.build());
  }

  private static Optional<SerdeFeature> getKeyWrapping(
      final boolean singleKey,
      final Format keyFormat
  ) {
    // Until ksqlDB supports WRAP_SINGLE_KEYS in the WITH clause, we explicitly set
    // UNWRAP_SINGLE_KEYS for any key format that supports both wrapping and unwrapping to avoid
    // ambiguity later:
    if (singleKey
        && keyFormat.supportsFeature(SerdeFeature.UNWRAP_SINGLES)
        && keyFormat.supportsFeature(SerdeFeature.WRAP_SINGLES)
    ) {
      return Optional.of(SerdeFeature.UNWRAP_SINGLES);
    }
    return Optional.empty();
  }

  private static Optional<SerdeFeature> getValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeFeatures explicitFeatures,
      final KsqlConfig ksqlConfig
  ) {
    final Optional<SerdeFeature> valueWrapping = explicitFeatures
        .findAny(SerdeFeatures.WRAPPING_FEATURES);

    if (valueWrapping.isPresent()) {
      validateExplicitValueWrapping(singleColumn, valueFormat, valueWrapping.get());
      return valueWrapping;
    }

    return getDefaultValueWrapping(singleColumn, valueFormat, ksqlConfig);
  }

  private static void validateExplicitValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeFeature wrappingFeature
  ) {
    if (!valueFormat.supportedFeatures().contains(wrappingFeature)) {
      final boolean value = wrappingFeature == SerdeFeature.WRAP_SINGLES;
      throw new KsqlException("Format '" + valueFormat.name() + "' "
          + "does not support '" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' set to '"
          + value + "'.");
    }

    if (!singleColumn) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }
  }

  private static Optional<SerdeFeature> getDefaultValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final KsqlConfig ksqlConfig
  ) {
    if (!singleColumn) {
      return Optional.empty();
    }

    final Boolean valueWrapping = ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES);
    if (valueWrapping == null) {
      return Optional.empty();
    }

    final SerdeFeature feature = valueWrapping
        ? SerdeFeature.WRAP_SINGLES
        : SerdeFeature.UNWRAP_SINGLES;

    if (!valueFormat.supportsFeature(feature)) {
      return Optional.empty();
    }

    return Optional.of(feature);
  }
}
