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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Set;

/**
 * Factory / Util class for building the {@link SerdeOption} sets required by the engine.
 */
public final class SerdeOptions {

  private static final ImmutableSet<SerdeOption> WRAPPING_OPTIONS = ImmutableSet.of(
      SerdeOption.WRAP_SINGLE_VALUES, SerdeOption.UNWRAP_SINGLE_VALUES
  );

  private SerdeOptions() {
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
  public static Set<SerdeOption> buildForCreateStatement(
      final LogicalSchema schema,
      final Format valueFormat,
      final Set<SerdeOption> explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleField = schema.valueConnectSchema().fields().size() == 1;
    return build(singleField, valueFormat, explicitOptions, ksqlConfig);
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
  public static Set<SerdeOption> buildForCreateAsStatement(
      final List<ColumnName> valueColumnNames,
      final Format valueFormat,
      final Set<SerdeOption> wrapSingleValues,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleField = valueColumnNames.size() == 1;
    return build(singleField, valueFormat, wrapSingleValues, ksqlConfig);
  }

  private static Set<SerdeOption> build(
      final boolean singleField,
      final Format valueFormat,
      final Set<SerdeOption> explicitOptions,
      final KsqlConfig ksqlConfig
  ) {
    final Set<SerdeOption> wrappingOptions = Sets.intersection(explicitOptions, WRAPPING_OPTIONS);
    if (wrappingOptions.isEmpty()) {
      if (!singleField) {
        return SerdeOption.none();
      }

      final Boolean valueWrapping = ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES);
      if (valueWrapping == null) {
        return SerdeOption.none();
      }

      final SerdeOption option = valueWrapping
          ? SerdeOption.WRAP_SINGLE_VALUES
          : SerdeOption.UNWRAP_SINGLE_VALUES;

      if (!valueFormat.supportsFeature(option.requiredFeature())) {
        return SerdeOption.none();
      }

      return SerdeOption.of(option);
    }

    if (wrappingOptions.size() != 1) {
      throw new IllegalStateException("Conflicting wrapping settings: " + explicitOptions);
    }

    final SerdeOption wrappingOption = Iterables.getOnlyElement(wrappingOptions);

    if (!valueFormat.supportedFeatures().contains(wrappingOption.requiredFeature())) {
      final boolean value = wrappingOption == SerdeOption.WRAP_SINGLE_VALUES;
      throw new KsqlException("Format '" + valueFormat.name() + "' "
          + "does not support '" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' set to '"
          + value + "'.");
    }

    if (!singleField) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }

    return explicitOptions;
  }
}
