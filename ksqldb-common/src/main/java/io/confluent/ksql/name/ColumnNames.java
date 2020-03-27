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

package io.confluent.ksql.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class ColumnNames {

  private static final String AGGREGATE_COLUMN_PREFIX = "KSQL_AGG_VARIABLE_";
  private static final String GENERATED_ALIAS_PREFIX = "KSQL_COL_";
  private static final String SYNTHESISED_COLUMN_PREFIX = "KSQL_SYNTH_";

  private static final Pattern GENERATED_ALIAS_PATTERN = Pattern
      .compile(GENERATED_ALIAS_PREFIX + "(\\d+)");

  private ColumnNames() {
  }

  /**
   * Used to generate a column name in an internal schema to hold the result of an UDAF.
   */
  public static ColumnName aggregateColumn(final int idx) {
    return ColumnName.of(AGGREGATE_COLUMN_PREFIX + idx);
  }

  /**
   * Create a generator that will build column aliases in the form {@code KSQL_COL_x}.
   *
   * <p>Names are guaranteed not to clash with any existing columns in the {@code sourceSchemas}.
   *
   * <p>Used where the user hasn't specified an alias for an expression in a SELECT. This generated
   * column names are exposed to the user in the output schema.
   *
   * @param sourceSchemas the stream of source schemas.
   * @return a generator of unique column names.
   */
  public static Supplier<ColumnName> columnAliasGenerator(
      final Stream<LogicalSchema> sourceSchemas
  ) {
    final Set<Integer> used = generatedAliasIndexes(sourceSchemas)
        .boxed()
        .collect(Collectors.toSet());

    return new AliasGenerator(0, used)::next;
  }

  /**
   * Used to generate a column name in an intermediate schema, e.g. for a column to hold values of a
   * table function. These are never exposed to the user
   */
  public static ColumnName synthesisedSchemaColumn(final int idx) {
    return ColumnName.of(SYNTHESISED_COLUMN_PREFIX + idx);
  }

  /**
   * Used to generate a column alias for a join where the a column with this name exists in both of
   * the sources.
   */
  public static ColumnName generatedJoinColumnAlias(
      final SourceName sourceName,
      final ColumnName ref
  ) {
    return ColumnName.of(sourceName.text() + "_" + ref.text());
  }

  /**
   * Used to generate the column name of an unaliased Struct Field, i.e. a `DereferenceExpression`.
   *
   * @param dereferenceExp the `DereferenceExpression` instance.
   * @return the column name.
   */
  public static ColumnName generatedStructFieldColumnName(final Object dereferenceExp) {
    final String text = dereferenceExp.toString();
    final String name = text.substring(text.indexOf(KsqlConstants.DOT) + 1)
        .replace(KsqlConstants.DOT, "_")
        .replace(KsqlConstants.STRUCT_FIELD_REF, "__");
    return ColumnName.of(name);
  }

  public static boolean isAggregate(final ColumnName name) {
    return name.text().startsWith(AGGREGATE_COLUMN_PREFIX);
  }

  private static OptionalInt extractGeneratedAliasIndex(final ColumnName columnName) {
    final Matcher matcher = GENERATED_ALIAS_PATTERN.matcher(columnName.text());
    return matcher.matches()
        ? OptionalInt.of(Integer.parseInt(matcher.group(1)))
        : OptionalInt.empty();
  }

  private static IntStream generatedAliasIndexes(final Stream<LogicalSchema> sourceSchema) {
    return sourceSchema
        .map(LogicalSchema::columns)
        .flatMap(List::stream)
        .map(Column::name)
        .map(ColumnNames::extractGeneratedAliasIndex)
        .filter(OptionalInt::isPresent)
        .mapToInt(OptionalInt::getAsInt);
  }

  @VisibleForTesting
  static final class AliasGenerator {

    private final Set<Integer> used;
    private int next;

    AliasGenerator(final int initial, final Set<Integer> used) {
      this.used = ImmutableSet.copyOf(used);
      this.next = initial;
    }

    ColumnName next() {
      return ColumnName.of(GENERATED_ALIAS_PREFIX + nextIndex());
    }

    private int nextIndex() {
      int idx;

      do {
        idx = next++;

        if (idx < 0) {
          throw new KsqlException("Wow, you've managed to use up all possible generated aliases. "
              + "Impressive! Please provide explicit aliases to some of your columns");
        }

      } while (used.contains(idx));

      return idx;
    }
  }
}
