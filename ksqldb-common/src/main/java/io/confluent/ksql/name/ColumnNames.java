/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.name;

public final class ColumnNames {

  private static final String AGGREGATE_COLUMN_PREFIX = "KSQL_AGG_VARIABLE_";
  private static final String GENERATED_ALIAS_PREFIX = "KSQL_COL_";
  private static final String SYNTHESISED_COLUMN_PREFIX = "KSQL_SYNTH_";

  private ColumnNames() {
  }

  /**
   * Used to generate a column name in an internal schema to hold the result of an UDAF.
   */
  public static ColumnName aggregateColumn(final int idx) {
    return ColumnName.of(AGGREGATE_COLUMN_PREFIX + idx);
  }

  /**
   * Where the user hasn't specified an alias for an expression in a SELECT we generate them
   * using this method. This value is exposed to the user in the output schema
   */
  public static ColumnName generatedColumnAlias(final int idx) {
    return ColumnName.of(GENERATED_ALIAS_PREFIX + idx);
  }

  /**
   * Used to generate a column name in an intermediate schema, e.g. for a column to hold
   * values of a table function. These are never exposed to the user
   */
  public static ColumnName synthesisedSchemaColumn(final int idx) {
    return ColumnName.of(SYNTHESISED_COLUMN_PREFIX + idx);
  }

  /**
   * Used to generate a column alias for a join where the a column with this name exists
   * in both of the sources.
   */
  public static ColumnName generatedJoinColumnAlias(
      final SourceName sourceName,
      final ColumnName ref
  ) {
    return ColumnName.of(sourceName.text() + "_" + ref.text());
  }

  public static boolean isAggregate(final ColumnName name) {
    return name.text().startsWith(AGGREGATE_COLUMN_PREFIX);
  }
}
