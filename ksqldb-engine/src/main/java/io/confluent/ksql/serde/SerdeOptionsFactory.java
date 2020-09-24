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

import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOptions.Builder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

/**
 * Validated set of {@link SerdeOption}s.
 *
 * <p>The class ensures no invalid combinations of options are possible.
 */
public final class SerdeOptionsFactory {

  private SerdeOptionsFactory() {
  }

  /**
   * Build serde options for C* and C*AS statements.
   *
   * @param schema the logical schema of the create statement.
   * @param keyFormat the format of the key.
   * @param valueFormat the format of the value.
   * @param explicitOptions explicitly set options.
   * @param ksqlConfig the session config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static SerdeOptions build(
      final LogicalSchema schema,
      final Format keyFormat,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final Builder builder = SerdeOptions.builder();

    getKeyWrapping(schema.key().size() == 1, keyFormat)
        .ifPresent(builder::add);

    getValueWrapping(schema.value().size() == 1, valueFormat, explicitOptions, ksqlConfig)
        .ifPresent(builder::add);

    return builder.build();
  }

  private static Optional<SerdeOption> getKeyWrapping(
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
      return Optional.of(SerdeOption.UNWRAP_SINGLE_KEYS);
    }
    return Optional.empty();
  }

  private static Optional<SerdeOption> getValueWrapping(
      final boolean singleValue,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final Optional<SerdeOption> valueWrapping = explicitOptions
        .findAny(SerdeOptions.VALUE_WRAPPING_OPTIONS);

    if (valueWrapping.isPresent()) {
      validateExplicitValueWrapping(singleValue, valueFormat, valueWrapping.get());
      return valueWrapping;
    }

    return getDefaultValueWrapping(singleValue, valueFormat, ksqlConfig);
  }

  private static void validateExplicitValueWrapping(
      final boolean singleValue,
      final Format valueFormat,
      final SerdeOption wrappingOption
  ) {
    if (!valueFormat.supportedFeatures().contains(wrappingOption.requiredFeature())) {
      final boolean value = wrappingOption == SerdeOption.WRAP_SINGLE_VALUES;
      throw new KsqlException("Format '" + valueFormat.name() + "' "
          + "does not support '" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' set to '"
          + value + "'.");
    }

    if (!singleValue) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }
  }

  private static Optional<SerdeOption> getDefaultValueWrapping(
      final boolean singleValue,
      final Format valueFormat,
      final KsqlConfig ksqlConfig
  ) {
    if (!singleValue) {
      return Optional.empty();
    }

    final Boolean valueWrapping = ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES);
    if (valueWrapping == null) {
      return Optional.empty();
    }

    final SerdeOption option = valueWrapping
        ? SerdeOption.WRAP_SINGLE_VALUES
        : SerdeOption.UNWRAP_SINGLE_VALUES;

    if (!valueFormat.supportsFeature(option.requiredFeature())) {
      return Optional.empty();
    }

    return Optional.of(option);
  }
}
