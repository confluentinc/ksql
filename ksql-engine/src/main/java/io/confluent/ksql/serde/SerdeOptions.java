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
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Factory / Util class for building the {@link SerdeOption} sets required by the engine.
 */
public final class SerdeOptions {

  private SerdeOptions() {
  }

  /**
   * Build the default options to be used by statements, should not be explicitly provided.
   *
   * @param ksqlConfig the system config containing defaults.
   * @return the set of default serde options.
   */
  public static Set<SerdeOption> buildDefaults(final KsqlConfig ksqlConfig) {
    final ImmutableSet.Builder<SerdeOption> options = ImmutableSet.builder();

    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES)) {
      options.add(SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return ImmutableSet.copyOf(options.build());
  }

  /**
   * Build serde options for {@code `CREATE STREAM`} and {@code `CREATE TABLE`} statements.
   *
   * @param schema the logical schema of the create statement.
   * @param valueFormat the format of the value.
   * @param wrapSingleValues explicitly set single value wrapping flag.
   * @param ksqlConfig the system config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static Set<SerdeOption> buildForCreateStatement(
      final LogicalSchema schema,
      final Format valueFormat,
      final Optional<Boolean> wrapSingleValues,
      final KsqlConfig ksqlConfig
  ) {
    final boolean singleField = schema.valueSchema().fields().size() == 1;
    final Set<SerdeOption> singleFieldDefaults = buildDefaults(ksqlConfig);
    return build(singleField, valueFormat, wrapSingleValues, singleFieldDefaults);
  }

  /**
   * Build serde options for {@code `CREATE STREAM AS SELECT`} and {@code `CREATE TABLE AS SELECT`}
   * statements.
   *
   * @param valueColumnNames the set of column names in the schema.
   * @param valueFormat the format of the value.
   * @param wrapSingleValues explicitly set single value wrapping flag.
   * @param singleFieldDefaults the defaults for single fields.
   * @return the set of serde options the statement defines.
   */
  public static Set<SerdeOption> buildForCreateAsStatement(
      final List<String> valueColumnNames,
      final Format valueFormat,
      final Optional<Boolean> wrapSingleValues,
      final Set<SerdeOption> singleFieldDefaults
  ) {
    final boolean singleField = valueColumnNames.size() == 1;
    return build(singleField, valueFormat, wrapSingleValues, singleFieldDefaults);
  }

  private static Set<SerdeOption> build(
      final boolean singleField,
      final Format valueFormat,
      final Optional<Boolean> wrapSingleValues,
      final Set<SerdeOption> singleFieldDefaults
  ) {
    if (!wrapSingleValues.isPresent()) {
      return singleField && singleFieldDefaults.contains(SerdeOption.UNWRAP_SINGLE_VALUES)
          ? SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES)
          : SerdeOption.none();
    }

    if (!valueFormat.supportsUnwrapping()) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' can not be used with format '"
          + valueFormat + "' as it does not support wrapping");
    }

    if (!singleField) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }

    final ImmutableSet.Builder<SerdeOption> options = ImmutableSet.builder();

    if (!wrapSingleValues.get()) {
      options.add(SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return options.build();
  }
}
