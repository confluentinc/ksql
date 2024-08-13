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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  public static final SqlType HEADERS_TYPE = SqlArray.of(
      SqlStruct.builder()
          .field("KEY", SqlTypes.STRING)
          .field("VALUE", SqlTypes.BYTES).build());

  private static final List<PseudoColumn> pseudoColumns = ImmutableList.of(
      PseudoColumn.of(
          ROWTIME_NAME,
          ROWTIME_TYPE,
          ROWTIME_PSEUDOCOLUMN_VERSION,
          false,
          false,
          false
      ),
      PseudoColumn.of(
          ROWPARTITION_NAME,
          ROWPARTITION_TYPE,
          ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION,
          true,
          true,
          true
      ),
      PseudoColumn.of(
          ROWOFFSET_NAME,
          ROWOFFSET_TYPE,
          ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION,
          true,
          true,
          true
      )
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
    return pseudoColumns.stream()
        .anyMatch(col -> col.name.equals(columnName));
  }

  public static Set<ColumnName> pseudoColumnNames() {
    return pseudoColumnNames(CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  public static Set<ColumnName> pseudoColumnNames(final int pseudoColumnVersion) {

    validatePseudoColumnVersion(pseudoColumnVersion);

    return pseudoColumns
        .stream()
        .filter(col -> col.version <= pseudoColumnVersion)
        .map(col -> col.name)
        .collect(Collectors.toSet());
  }

  public static boolean isSystemColumn(final ColumnName columnName) {
    return isSystemColumn(columnName, CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  public static boolean isSystemColumn(final ColumnName columnName, final int pseudoColumnVersion) {
    return systemColumnNames(pseudoColumnVersion).contains(columnName);
  }

  public static Set<ColumnName> systemColumnNames() {
    return systemColumnNames(CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  public static Set<ColumnName> systemColumnNames(final int pseudoColumnVersion) {
    return Stream.concat(
        WINDOW_BOUNDS_COLUMN_NAMES.stream(),
        pseudoColumnNames(pseudoColumnVersion).stream()
    ).collect(Collectors.toSet());
  }

  /**
   * Checks if a given pseudo column name is associated with a pseudo column that must be
   * materialized for table joins
   *
   * @param columnName the pseudo column name provided
   * @return if the name is associated with a pseudo column that must be materialized for table
   * @throws IllegalArgumentException when column name is not associated with a pseudo column
   */
  public static boolean mustBeMaterializedForTableJoins(final ColumnName columnName) {
    return pseudoColumns
        .stream()
        .filter(col -> col.name.equals(columnName))
        .findFirst().orElseThrow(IllegalArgumentException::new)
        .mustBeMaterializedForTableJoins;
  }

  public static boolean isDisallowedForInsertValues(final ColumnName columnName) {
    return pseudoColumns
        .stream()
        .filter(col -> col.name.equals(columnName))
        .anyMatch(col -> col.isDisallowedForInsertValues);
  }

  public static boolean isDisallowedInPullOrScalablePushQueries(
      final ColumnName columnName
  ) {
    return isDisallowedInPullOrScalablePushQueries(columnName, CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  public static boolean isDisallowedInPullOrScalablePushQueries(
      final ColumnName columnName,
      final int pseudoColumnVersion
  ) {
    return pseudoColumns
        .stream()
        .filter(col -> col.version <= pseudoColumnVersion)
        .filter(col -> col.isDisallowedInPullAndScalablePushQueries)
        .anyMatch(col -> col.name.equals(columnName));
  }

  private static void validatePseudoColumnVersion(final int pseudoColumnVersionNumber) {
    if (pseudoColumnVersionNumber < LEGACY_PSEUDOCOLUMN_VERSION_NUMBER
        || pseudoColumnVersionNumber > CURRENT_PSEUDOCOLUMN_VERSION_NUMBER) {
      throw new IllegalArgumentException("Invalid pseudoColumnVersionNumber provided");
    }
  }

  /**
   * This class was added with ROWPARTITION and ROWOFFSET to address the growing
   * number of differences between the columns and ROWTIME. To add future PseudoColumns,
   * one will need to consider each of the fields below, add a new pseudo column version
   * (by incrementing CURRENT_PSEUDOCOLUMN_VERSION_NUMBER and setting the newest PseudoColumns
   * to use the new CURRENT), and make the necessary changes in LogicalSchema.java and the
   * value transformers in SourceBuilder.java.
   */
  @Immutable
  private static final class PseudoColumn {

    final ColumnName name;
    final SqlType type;
    final int version;
    final boolean mustBeMaterializedForTableJoins;
    final boolean isDisallowedForInsertValues;
    final boolean isDisallowedInPullAndScalablePushQueries;

    private PseudoColumn(
        final ColumnName name,
        final SqlType type,
        final int version,
        final boolean mustBeMaterializedForTableJoins,
        final boolean isDisallowedForInsertValues,
        final boolean isDisallowedInPullAndScalablePushQueries
    ) {
      this.name = requireNonNull(name, "name");
      this.type = requireNonNull(type, "type");
      this.version = version;
      this.mustBeMaterializedForTableJoins = mustBeMaterializedForTableJoins;
      this.isDisallowedForInsertValues = isDisallowedForInsertValues;
      this.isDisallowedInPullAndScalablePushQueries = isDisallowedInPullAndScalablePushQueries;
    }

    private static PseudoColumn of(
        final ColumnName name,
        final SqlType type,
        final int version,
        final boolean mustBeMaterializedForTableJoins,
        final boolean isDisallowedForInsertValues,
        final boolean isDisallowedInPullAndScalablePushQueries
    ) {
      return new PseudoColumn(
          name,
          type,
          version,
          mustBeMaterializedForTableJoins,
          isDisallowedForInsertValues,
          isDisallowedInPullAndScalablePushQueries
      );
    }
  }
}
