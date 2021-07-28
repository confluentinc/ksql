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

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Set;

public final class SystemColumns {

  public static final ColumnName ROWKEY_NAME = ColumnName.of("ROWKEY");

  public static final ColumnName ROWTIME_NAME = ColumnName.of("ROWTIME");
  public static final SqlType ROWTIME_TYPE = SqlTypes.BIGINT;

  public static final ColumnName WINDOWSTART_NAME = ColumnName.of("WINDOWSTART");
  public static final ColumnName WINDOWEND_NAME = ColumnName.of("WINDOWEND");
  public static final SqlType WINDOWBOUND_TYPE = SqlTypes.BIGINT;

  public static final int LEGACY_PSEUDOCOLUMN_VERSION_NUMBER = 0;
  public static final int CURRENT_PSEUDOCOLUMN_VERSION_NUMBER = 5;

  private static final Set<ColumnName> PSEUDO_COLUMN_NAMES = ImmutableSet.of(
      ROWTIME_NAME
  );

  private static final Set<ColumnName> WINDOW_BOUNDS_COLUMN_NAMES = ImmutableSet.of(
      WINDOWSTART_NAME,
      WINDOWEND_NAME
  );

  private static final Set<ColumnName> SYSTEM_COLUMN_NAMES = ImmutableSet.<ColumnName>builder()
      .addAll(PSEUDO_COLUMN_NAMES)
      .addAll(WINDOW_BOUNDS_COLUMN_NAMES)
      .build();

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

  @SuppressFBWarnings(
      value = "MS_EXPOSE_REP",
      justification = "PSEUDO_COLUMN_NAMES is ImmutableSet"
  )
  public static Set<ColumnName> pseudoColumnNames() {
    return PSEUDO_COLUMN_NAMES;
  }

  public static boolean isSystemColumn(final ColumnName columnName) {
    return systemColumnNames().contains(columnName);
  }

  @SuppressFBWarnings(
      value = "MS_EXPOSE_REP",
      justification = "SYSTEM_COLUMN_NAMES is ImmutableSet"
  )
  public static Set<ColumnName> systemColumnNames() {
    return SYSTEM_COLUMN_NAMES;
  }
}
