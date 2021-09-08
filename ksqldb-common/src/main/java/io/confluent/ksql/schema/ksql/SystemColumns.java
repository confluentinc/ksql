/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Set;

public final class SystemColumns {

  public static final ColumnName ROWKEY_NAME = ColumnName.of("ROWKEY");

  public static final ColumnName ROWTIME_NAME = ColumnName.of("ROWTIME");
  public static final SqlType ROWTIME_TYPE = SqlTypes.BIGINT;

  public static final ColumnName ROWPARTITION_NAME = ColumnName.of("ROWPARTITION");
  public static final SqlType ROWPARTITION_TYPE = SqlTypes.INTEGER;

  public static final ColumnName ROWOFFSET_NAME = ColumnName.of("ROWOFFSET");
  public static final SqlType ROWOFFSET_TYPE = SqlTypes.BIGINT;

  public static final ColumnName WINDOWSTART_NAME = ColumnName.of("WINDOWSTART");
  public static final ColumnName WINDOWEND_NAME = ColumnName.of("WINDOWEND");

  public static final SqlType WINDOWBOUND_TYPE = SqlTypes.BIGINT;

  public static final int ROWTIME_PSEUDOCOLUMN_VERSION = 0;
  public static final int ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION = 1;

  public static final int LEGACY_PSEUDOCOLUMN_VERSION_NUMBER = ROWTIME_PSEUDOCOLUMN_VERSION;
  public static final int CURRENT_PSEUDOCOLUMN_VERSION_NUMBER =
      ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION;

  private static final Set<ColumnName> WINDOW_BOUNDS_COLUMN_NAMES = ImmutableSet.of(
      WINDOWSTART_NAME,
      WINDOWEND_NAME
  );

  private static final Set<ColumnName> PSEUDO_COLUMN_VERSION_ZERO_NAMES
      = ImmutableSet.<ColumnName>builder()
      .add(ROWTIME_NAME)
      .build();

  private static final Set<ColumnName> PSEUDO_COLUMN_VERSION_ONE_NAMES
      = ImmutableSet.<ColumnName>builder()
      .addAll(PSEUDO_COLUMN_VERSION_ZERO_NAMES)
      .add(ROWPARTITION_NAME)
      .add(ROWOFFSET_NAME)
      .build();

  private static final Map<Integer, Set<ColumnName>> PSEUDO_COLUMN_NAMES_BY_VERSION =
      ImmutableMap.of(
          0, PSEUDO_COLUMN_VERSION_ZERO_NAMES,
          1, PSEUDO_COLUMN_VERSION_ONE_NAMES
      );

  private static final Set<ColumnName> MUST_BE_MATERIALIZED_FOR_TABLE_JOINS =
      ImmutableSet.of(
          ROWPARTITION_NAME,
          ROWOFFSET_NAME
      );

  private static final Map<Integer, Set<ColumnName>> SYSTEM_COLUMN_NAMES_BY_VERSION =
      ImmutableMap.of(
          0, buildColumns(0),
          1, buildColumns(1)
      );

  private SystemColumns() {
  }

  public static boolean isWindowBound(final ColumnName columnName) {
    return windowBoundsColumnNames().contains(columnName);
  }

  @SuppressFBWarnings(
      value = "MS_EXPOSE_REP",
      justification = "WINDOW_BOUNDS_COLUMN_NAMES is ImmutableSet"
  )
  public static Set<ColumnName> windowBoundsColumnNames() {
    return WINDOW_BOUNDS_COLUMN_NAMES;
  }

  @VisibleForTesting
  static boolean isPseudoColumn(final ColumnName columnName, final int pseudoColumnVersion) {
    return pseudoColumnNames(pseudoColumnVersion).contains(columnName);
  }

  public static boolean isPseudoColumn(
      final ColumnName columnName,
      final KsqlConfig ksqlConfig
  ) {
    return isPseudoColumn(columnName, getPseudoColumnVersionFromConfig(ksqlConfig));
  }

  public static Set<ColumnName> pseudoColumnNames(final int pseudoColumnVersion) {
    if (!PSEUDO_COLUMN_NAMES_BY_VERSION.containsKey(pseudoColumnVersion)) {
      throw new KsqlException(
          "Provided pseudoColumnVersion has no corresponding columns defined");
    }
    return PSEUDO_COLUMN_NAMES_BY_VERSION.get(pseudoColumnVersion);
  }

  public static Set<ColumnName> pseudoColumnNames(final KsqlConfig ksqlConfig) {
    return pseudoColumnNames(getPseudoColumnVersionFromConfig(ksqlConfig));
  }

  public static boolean isSystemColumn(final ColumnName columnName, final int pseudoColumnVersion) {
    return systemColumnNames(pseudoColumnVersion).contains(columnName);
  }

  public static boolean isSystemColumn(final ColumnName columnName, final KsqlConfig ksqlConfig) {
    return isSystemColumn(columnName, getPseudoColumnVersionFromConfig(ksqlConfig));
  }

  @SuppressFBWarnings(
      value = "MS_EXPOSE_REP",
      justification = "SYSTEM_COLUMN_NAMES is ImmutableSet"
  )
  private static Set<ColumnName> systemColumnNames(final int pseudoColumnVersion) {
    return SYSTEM_COLUMN_NAMES_BY_VERSION.get(pseudoColumnVersion);
  }

  public static boolean mustBeMaterializedForTableJoins(final ColumnName columnName) {
    return MUST_BE_MATERIALIZED_FOR_TABLE_JOINS.contains(columnName);
  }

  private static Set<ColumnName> buildColumns(final int pseudoColumnVersion) {
    return ImmutableSet.<ColumnName>builder()
        .addAll(pseudoColumnNames(pseudoColumnVersion))
        .addAll(WINDOW_BOUNDS_COLUMN_NAMES)
        .build();
  }

    private static int getPseudoColumnVersionFromConfig(final KsqlConfig ksqlConfig) {
      return ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)
          ? CURRENT_PSEUDOCOLUMN_VERSION_NUMBER
          : LEGACY_PSEUDOCOLUMN_VERSION_NUMBER;
    }
}
