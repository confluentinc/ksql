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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
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
      ROWTIME_PSEUDOCOLUMN_VERSION;

  private static final Set<ColumnName> WINDOW_BOUNDS_COLUMN_NAMES = ImmutableSet.of(
      WINDOWSTART_NAME,
      WINDOWEND_NAME
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

  public static boolean isPseudoColumn(final ColumnName columnName) {
    return pseudoColumnNames().contains(columnName);
  }

  public static Set<ColumnName> pseudoColumnNames(final int pseudoColumnVersion) {
    if (!PseudoColumns.PSEUDO_COLUMN_NAMES_BY_VERSION.containsKey(pseudoColumnVersion)) {
      throw new KsqlException("Provided pseudoColumnVersion has no corresponding columns defined");
    }
    return PseudoColumns.PSEUDO_COLUMN_NAMES_BY_VERSION.get(pseudoColumnVersion);
  }

  @SuppressFBWarnings(
      value = "MS_EXPOSE_REP",
      justification = "PSEUDO_COLUMN_NAMES is ImmutableSet"
  )
  public static Set<ColumnName> pseudoColumnNames() {
    return pseudoColumnNames(CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  public static boolean isSystemColumn(final ColumnName columnName) {
    return systemColumnNames().contains(columnName);
  }

  @SuppressFBWarnings(
      value = "MS_EXPOSE_REP",
      justification = "SYSTEM_COLUMN_NAMES is ImmutableSet"
  )
  public static Set<ColumnName> systemColumnNames() {
    return systemColumnNames(CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  public static Set<ColumnName> systemColumnNames(final int pseudoColumnVersion) {
    return SYSTEM_COLUMN_NAMES_BY_VERSION.get(pseudoColumnVersion);
  }

  private static Set<ColumnName> buildColumns(final int pseudoColumnVersion) {
    return ImmutableSet.<ColumnName>builder()
        .addAll(pseudoColumnNames(pseudoColumnVersion))
        .addAll(WINDOW_BOUNDS_COLUMN_NAMES)
        .build();
  }

  static class PseudoColumns {

    private static final Set<ColumnName> VERSION_ZERO_NAMES;
    private static final Set<ColumnName> VERSION_ONE_NAMES;

    static {
      VERSION_ZERO_NAMES = new HashSet<>();
      VERSION_ZERO_NAMES.add(ROWTIME_NAME);

      VERSION_ONE_NAMES = new HashSet<>(VERSION_ZERO_NAMES);
      VERSION_ONE_NAMES.add(ROWOFFSET_NAME);
      VERSION_ONE_NAMES.add(ROWPARTITION_NAME);
    }

    private static final Map<Integer, Set<ColumnName>> PSEUDO_COLUMN_NAMES_BY_VERSION =
        ImmutableMap.of(
            0, VERSION_ZERO_NAMES,
            1, VERSION_ONE_NAMES
        );

  }
}
