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

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOptions.Builder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
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
   * Build serde options for {@code `CREATE STREAM`} and {@code `CREATE TABLE`} statements.
   *
   * @param schema the logical schema of the create statement.
   * @param keyFormat the format of the key.
   * @param valueFormat the format of the value.
   * @param explicitOptions explicitly set options.
   * @param ksqlConfig the session config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static SerdeOptions buildForCreateStatement(
      final LogicalSchema schema,
      final Format keyFormat,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleColumn = schema.valueConnectSchema().fields().size() == 1;
    return build(singleColumn, keyFormat, valueFormat, explicitOptions, ksqlConfig);
  }

  /**
   * Build serde options for {@code `CREATE STREAM AS SELECT`} and {@code `CREATE TABLE AS SELECT`}
   * statements.
   *
   * @param valueColumnNames the set of column names in the schema.
   * @param keyFormat the format of the key.
   * @param valueFormat the format of the value.
   * @param wrapSingleValues explicitly set single value wrapping flag.
   * @param ksqlConfig the session config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static SerdeOptions buildForCreateAsStatement(
      final List<ColumnName> valueColumnNames,
      final Format keyFormat,
      final Format valueFormat,
      final SerdeOptions wrapSingleValues,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleColumn = valueColumnNames.size() == 1;
    return build(singleColumn, keyFormat, valueFormat, wrapSingleValues, ksqlConfig);
  }

  private static SerdeOptions build(
      final boolean singleColumn,
      final Format keyFormat,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final Builder builder = SerdeOptions.builder();

    getKeyWrapping(keyFormat)
        .ifPresent(builder::add);

    getValueWrapping(singleColumn, valueFormat, explicitOptions, ksqlConfig)
        .ifPresent(builder::add);

    return builder.build();
  }

  private static Optional<SerdeOption> getKeyWrapping(final Format keyFormat) {
    // Until ksqlDB supports WRAP_SINGLE_KEYS in the WITH clause, we explicitly set
    // UNWRAP_SINGLE_KEYS for any key format that supports both wrapping and unwrapping to avoid
    // ambiguity later:
    if (keyFormat.supportsFeature(SerdeFeature.UNWRAP_SINGLES)
        && keyFormat.supportsFeature(SerdeFeature.WRAP_SINGLES)
    ) {
      return Optional.of(SerdeOption.UNWRAP_SINGLE_KEYS);
    }
    return Optional.empty();
  }

  private static Optional<SerdeOption> getValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final Optional<SerdeOption> valueWrapping = explicitOptions.valueWrapping();
    if (valueWrapping.isPresent()) {
      validateExplicitValueWrapping(singleColumn, valueFormat, valueWrapping.get());
      return valueWrapping;
    }

    return getDefaultValueWrapping(singleColumn, valueFormat, ksqlConfig);
  }

  private static void validateExplicitValueWrapping(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeOption wrappingOption
  ) {
    if (!valueFormat.supportedFeatures().contains(wrappingOption.requiredFeature())) {
      final boolean value = wrappingOption == SerdeOption.WRAP_SINGLE_VALUES;
      throw new KsqlException("Format '" + valueFormat.name() + "' "
          + "does not support '" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' set to '"
          + value + "'.");
    }

    if (!singleColumn) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }
  }

  private static Optional<SerdeOption> getDefaultValueWrapping(
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

    final SerdeOption option = valueWrapping
        ? SerdeOption.WRAP_SINGLE_VALUES
        : SerdeOption.UNWRAP_SINGLE_VALUES;

    if (!valueFormat.supportsFeature(option.requiredFeature())) {
      return Optional.empty();
    }

    return Optional.of(option);
  }
}
