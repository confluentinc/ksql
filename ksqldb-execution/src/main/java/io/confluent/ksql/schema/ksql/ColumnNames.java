/*
 * Copyright 2021 Confluent Inc.
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ColumnNames {

  private static final String AGGREGATE_COLUMN_PREFIX = "KSQL_AGG_VARIABLE_";
  private static final String GENERATED_ALIAS_PREFIX = "KSQL_COL";
  private static final String SYNTHESISED_COLUMN_PREFIX = "KSQL_SYNTH_";
  private static final String SYNTHETIC_JOIN_KEY_COLUMN_PRIFIX = "ROWKEY";

  private static final String NAME = "name";
  private static final String NUMBER = "number";

  /**
   * Pattern to match any column name that ends in an underscore and some number, e.g. COL_1 or
   * NAME_26.
   *
   * <p>The pattern has two named capture groups:
   * <ul>
   *   <li><b>name</b>: captures the text before the underscore</li>
   *   <li><b>number</b>: captures the number after the underscore</li>
   * </ul>
   */
  private static final Pattern NUMBERED_COLUMN_PATTERN = Pattern
      .compile("(?<" + NAME + ">.*?)?(?:_(?<" + NUMBER + ">\\d+))?");

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
  public static ColumnAliasGenerator columnAliasGenerator(
      final Stream<LogicalSchema> sourceSchemas
  ) {
    final Map<String, AliasGenerator> used = buildAliasGenerators(sourceSchemas);

    return new ColumnAliasGenerator() {
      final StructFieldAliasGenerator generator = new StructFieldAliasGenerator(used);

      @Override
      public ColumnName nextKsqlColAlias() {
        return used.get(GENERATED_ALIAS_PREFIX).next();
      }

      @Override
      public ColumnName uniqueAliasFor(final Expression expression) {
        return expression instanceof DereferenceExpression
            ? generator.next((DereferenceExpression) expression)
            : nextKsqlColAlias();
      }
    };
  }

  /**
   * Short hand helper for when only a single alias is required.
   *
   * <p>Short hand for:
   * <pre>
   * {@code
   *   ColumnNames.columnAliasGenerator(Arrays.stream(schemas)).uniqueAliasFor(e);
   * }
   * </pre>
   *
   * @param e the expression an alias is needed for.
   * @param schemas the source schemas.
   * @return a unique column name.
   */
  public static ColumnName uniqueAliasFor(final Expression e, final LogicalSchema... schemas) {
    if (schemas.length == 0) {
      throw new IllegalArgumentException("At least once schema should be provided");
    }

    return columnAliasGenerator(Arrays.stream(schemas)).uniqueAliasFor(e);
  }

  /**
   * Short hand helper for when only a single generated alias is required.
   *
   * <p>Short hand for:
   * <pre>
   * {@code
   *   ColumnNames.columnAliasGenerator(Arrays.stream(schemas)).nextKsqlColAlias();
   * }
   * </pre>
   *
   * @param schemas the source schemas.
   * @return a unique column name.
   */
  public static ColumnName nextKsqlColAlias(final LogicalSchema... schemas) {
    if (schemas.length == 0) {
      throw new IllegalArgumentException("At least once schema should be provided");
    }

    return columnAliasGenerator(Arrays.stream(schemas)).nextKsqlColAlias();
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

  public static ColumnName generateSyntheticJoinKey(final Stream<LogicalSchema> schemas) {
    final AliasGenerator generator = buildAliasGenerators(schemas)
        .getOrDefault(
            SYNTHETIC_JOIN_KEY_COLUMN_PRIFIX,
            new AliasGenerator(0, SYNTHETIC_JOIN_KEY_COLUMN_PRIFIX, ImmutableSet.of())
        );

    return generator.next();
  }

  /**
   * Determines is the supplied {@code columnName} could be a generated column name in the form
   * {@code KSQL_COL_x}.
   *
   * <p>Of course, just because a column name matches this pattern doesn't mean it is a generated
   * column name. The user could have chosen this name directly, or it could have been generated
   * in a different statement.
   *
   * @param columnName the name to test
   * @return {@code true} if it starts with {@link #SYNTHETIC_JOIN_KEY_COLUMN_PRIFIX}.
   */
  public static boolean maybeSyntheticJoinKey(final ColumnName columnName) {
    return columnName.text().startsWith(SYNTHETIC_JOIN_KEY_COLUMN_PRIFIX);
  }

  /**
   * @param name the column name to test
   * @return {@code true} if it starts with {@link #AGGREGATE_COLUMN_PREFIX}.
   */
  public static boolean isAggregate(final ColumnName name) {
    return name.text().startsWith(AGGREGATE_COLUMN_PREFIX);
  }

  private static Map<String, AliasGenerator> buildAliasGenerators(
      final Stream<LogicalSchema> sourceSchema
  ) {
    final Map<String, Set<Integer>> used = sourceSchema
        .map(LogicalSchema::columns)
        .flatMap(List::stream)
        .map(Column::name)
        .map(ColumnName::text)
        .map(NUMBERED_COLUMN_PATTERN::matcher)
        .collect(Collectors.toMap(
            ColumnNames::columnNameWithoutNumber,
            ColumnNames::extractNumber,
            Sets::union
        ));

    final Map<String, AliasGenerator> generators = used.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> new AliasGenerator(
                0, e.getKey(),
                e.getValue()
            )
        ));

    generators.computeIfAbsent(
        GENERATED_ALIAS_PREFIX,
        k -> new AliasGenerator(0, k, ImmutableSet.of())
    );

    return generators;
  }

  private static String columnNameWithoutNumber(final Matcher matcher) {
    if (!matcher.matches()) {
      throw new IllegalStateException();
    }

    return matcher.group(NAME);
  }

  private static Set<Integer> extractNumber(final Matcher matcher) {
    if (!matcher.matches()) {
      throw new IllegalStateException();
    }

    final String number = matcher.group(NUMBER);
    return number == null
        ? ImmutableSet.of(0)
        : ImmutableSet.of(Integer.parseInt(number));
  }

  @VisibleForTesting
  static final class AliasGenerator {

    private final Set<Integer> used;
    private final String prefix;
    private final boolean dropZero;
    private int next;

    AliasGenerator(
        final int initial,
        final String prefix,
        final Set<Integer> used
    ) {
      this.used = ImmutableSet.copyOf(used);
      this.next = initial;
      this.prefix = Objects.requireNonNull(prefix, "prefix");
      this.dropZero = !prefix.equals(GENERATED_ALIAS_PREFIX);
    }

    ColumnName next() {
      final int idx = nextIndex();
      return idx == 0 && dropZero
          ? ColumnName.of(prefix)
          : ColumnName.of(prefix + "_" + idx);
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

  private static final class StructFieldAliasGenerator {

    private final Map<String, AliasGenerator> usedFieldNames;

    StructFieldAliasGenerator(final Map<String, AliasGenerator> used) {
      this.usedFieldNames = new HashMap<>(used);
    }

    ColumnName next(final DereferenceExpression e) {
      final String key = columnNameWithoutNumber(NUMBERED_COLUMN_PATTERN.matcher(e.getFieldName()));
      final AliasGenerator generator = usedFieldNames
          .computeIfAbsent(key, k -> new AliasGenerator(0, k, ImmutableSet.of()));

      return generator.next();
    }
  }
}
