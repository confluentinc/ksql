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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;

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
   * @param valueFormat the format of the value.
   * @param explicitOptions explicitly set options.
   * @param ksqlConfig the session config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static SerdeOptions buildForCreateStatement(
      final LogicalSchema schema,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleColumn = schema.valueConnectSchema().fields().size() == 1;
    return build(singleColumn, valueFormat, explicitOptions, ksqlConfig);
  }

  /**
   * Build serde options for {@code `CREATE STREAM AS SELECT`} and {@code `CREATE TABLE AS SELECT`}
   * statements.
   *
   * @param valueColumnNames the set of column names in the schema.
   * @param valueFormat the format of the value.
   * @param wrapSingleValues explicitly set single value wrapping flag.
   * @param ksqlConfig the session config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static SerdeOptions buildForCreateAsStatement(
      final List<ColumnName> valueColumnNames,
      final Format valueFormat,
      final SerdeOptions wrapSingleValues,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleColumn = valueColumnNames.size() == 1;
    return build(singleColumn, valueFormat, wrapSingleValues, ksqlConfig);
  }

  private static SerdeOptions build(
      final boolean singleColumn,
      final Format valueFormat,
      final SerdeOptions explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    if (!explicitOptions.valueWrapping().isPresent()) {
      if (!singleColumn) {
        return SerdeOptions.of();
      }

      final Boolean valueWrapping = ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES);
      if (valueWrapping == null) {
        return SerdeOptions.of();
      }

      final SerdeOption option = valueWrapping
          ? SerdeOption.WRAP_SINGLE_VALUES
          : SerdeOption.UNWRAP_SINGLE_VALUES;

      if (!valueFormat.supportsFeature(option.requiredFeature())) {
        return SerdeOptions.of();
      }

      return SerdeOptions.of(option);
    }

    final SerdeOption wrappingOption = explicitOptions.valueWrapping().get();

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

    return explicitOptions;
  }
}
