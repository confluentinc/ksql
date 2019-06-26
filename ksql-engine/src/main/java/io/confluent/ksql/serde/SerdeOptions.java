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
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.LiteralUtil;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
   * @param properties the WITH clause properties of the statement.
   * @param ksqlConfig the system config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static Set<SerdeOption> buildForCreateStatement(
      final LogicalSchema schema,
      final CreateSourceProperties properties,
      final KsqlConfig ksqlConfig
  ) {
    final Format valueFormat = properties.getValueFormat();

    final ImmutableSet.Builder<SerdeOption> options = ImmutableSet.builder();

    final boolean singleValueField = schema.valueSchema().fields().size() == 1;

    if (properties.getWrapSingleValues().isPresent() && !singleValueField) {
      throw new KsqlException("'" + DdlConfig.WRAP_SINGLE_VALUE + "' "
          + "is only valid for single-field value schemas");
    }

    if (properties.getWrapSingleValues().isPresent() && !valueFormat.supportsUnwrapping()) {
      throw new KsqlException("'" + DdlConfig.WRAP_SINGLE_VALUE + "' can not be used with format '"
          + valueFormat + "' as it does not support wrapping");
    }

    final Set<SerdeOption> defaults = buildDefaults(ksqlConfig);

    if (singleValueField && !properties.getWrapSingleValues()
        .orElseGet(() -> !defaults.contains(SerdeOption.UNWRAP_SINGLE_VALUES))
    ) {
      options.add(SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return Collections.unmodifiableSet(options.build());
  }

  /**
   * Build serde options for {@code `CREATE STREAM AS SELECT`} and {@code `CREATE TABLE AS SELECT`}
   * statements.
   *
   * @param columnNames the set of column names in the schema.
   * @param properties the WITH clause properties of the statement.
   * @param valueFormat the format of the value.
   * @param singleFieldDefaults the defaults for single fields.
   * @return the set of serde options the statement defines.
   */
  public static Set<SerdeOption> buildForCreateAsStatement(
      final List<String> columnNames,
      final Map<String, Expression> properties,
      final Format valueFormat,
      final Set<SerdeOption> singleFieldDefaults
  ) {
    final boolean singleField = columnNames.size() == 1;

    final Expression exp = properties.get(DdlConfig.WRAP_SINGLE_VALUE);
    if (exp == null) {
      return singleField && singleFieldDefaults.contains(SerdeOption.UNWRAP_SINGLE_VALUES)
          ? SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES)
          : SerdeOption.none();
    }

    if (!valueFormat.supportsUnwrapping()) {
      throw new KsqlException("'" + DdlConfig.WRAP_SINGLE_VALUE + "' can not be used with format '"
          + valueFormat + "' as it does not support wrapping");
    }

    if (!singleField) {
      throw new KsqlException("'" + DdlConfig.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }

    if (!(exp instanceof Literal)) {
      throw new KsqlException(DdlConfig.WRAP_SINGLE_VALUE
          + " set in the WITH clause must be set to a literal");
    }

    final ImmutableSet.Builder<SerdeOption> options = ImmutableSet.builder();

    if (!LiteralUtil.toBoolean(((Literal) exp), DdlConfig.WRAP_SINGLE_VALUE)) {
      options.add(SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return options.build();
  }
}
