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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.schema.ksql.ColumnNames.AliasGenerator;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

public class ColumnNamesTest {

  private static final Optional<NodeLocation> LOCATION = Optional.empty();
  private static final SourceName SOURCE_NAME = SourceName.of("Bob");
  private static final Expression STRUCT_COLUMN = new QualifiedColumnReferenceExp(
      SOURCE_NAME,
      ColumnName.of("Struct_Name")
  );

  @Test
  public void shouldStartGeneratingFromZeroIfSourceSchemasHaveNoGeneratedAliases() {
    // Given:
    final ColumnAliasGenerator generator = ColumnNames
        .columnAliasGenerator(Stream.of(LogicalSchema.builder().build()));

    // When:
    final ColumnName result = generator.nextKsqlColAlias();

    // Then:
    assertThat(result, is(ColumnName.of("KSQL_COL_0")));
  }

  @Test
  public void shouldAvoidClashesWithSourceColumnNames() {
    // Given:
    final ColumnAliasGenerator generator = ColumnNames
        .columnAliasGenerator(Stream.of(LogicalSchema.builder()
            .keyColumn(ColumnName.of("Fred"), SqlTypes.STRING)
            .keyColumn(ColumnName.of("KSQL_COL_3"), SqlTypes.STRING)
            .keyColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("George"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("KSQL_COL_5"), SqlTypes.STRING)
            .build()
        ));

    // When:
    final List<ColumnName> result = IntStream.range(0, 5)
        .mapToObj(idx -> generator.nextKsqlColAlias())
        .collect(Collectors.toList());

    // Then:
    assertThat(result, contains(
        ColumnName.of("KSQL_COL_0"),
        ColumnName.of("KSQL_COL_2"),
        ColumnName.of("KSQL_COL_4"),
        ColumnName.of("KSQL_COL_6"),
        ColumnName.of("KSQL_COL_7")
    ));
  }

  @Test
  public void shouldThrowIfIndexOverflows() {
    // Given:
    final AliasGenerator generator =
        new AliasGenerator(Integer.MAX_VALUE, "prefix", ImmutableSet.of());

    generator.next(); // returns MAX_VALUE.

    // When:
    assertThrows(KsqlException.class, generator::next);
  }

  @Test
  public void shouldGenerateSimpleStructColumnName() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder().build();

    final ColumnAliasGenerator generator = ColumnNames
        .columnAliasGenerator(Stream.of(schema));

    final DereferenceExpression exp =
        new DereferenceExpression(LOCATION, STRUCT_COLUMN, "Field_name");

    // When:
    final ColumnName result1 = generator.uniqueAliasFor(exp);
    final ColumnName result2 = generator.uniqueAliasFor(exp);

    // Then:
    assertThat(result1, is(ColumnName.of("Field_name")));
    assertThat(result2, is(ColumnName.of("Field_name_1")));
  }

  @Test
  public void shouldEnsureGeneratedStructAliasesDoNotClash() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("someField_2"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("other"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("someField_3"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("another"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("someField"), SqlTypes.STRING)
        .build();

    final ColumnAliasGenerator generator = ColumnNames.columnAliasGenerator(Stream.of(schema));

    final DereferenceExpression fieldWithNameClash =
        new DereferenceExpression(LOCATION, STRUCT_COLUMN, "someField");

    // When:
    final ColumnName result1 = generator.uniqueAliasFor(fieldWithNameClash);
    final ColumnName result2 = generator.uniqueAliasFor(fieldWithNameClash);

    // Then:
    assertThat(result1, is(ColumnName.of("someField_1")));
    assertThat(result2, is(ColumnName.of("someField_4")));
  }

  @Test
  public void shouldEnsureGeneratedStructAliasesDoNotClashForNumericFieldNames() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("1"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("2"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("3_1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("4"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("5"), SqlTypes.STRING)
        .build();

    final ColumnAliasGenerator generator = ColumnNames.columnAliasGenerator(Stream.of(schema));

    final DereferenceExpression fieldWithNameClash =
        new DereferenceExpression(LOCATION, STRUCT_COLUMN, "3");

    // When:
    final ColumnName result1 = generator.uniqueAliasFor(fieldWithNameClash);
    final ColumnName result2 = generator.uniqueAliasFor(fieldWithNameClash);

    // Then:
    assertThat(result1, is(ColumnName.of("3")));
    assertThat(result2, is(ColumnName.of("3_2")));
  }

  @Test
  public void shouldGenerateStructAliasesForSchemasWithAnyColumnNames() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("12345"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("12startsWithNumbers"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("12startsAndEndsWithNumbers84"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("numbers12In45Middle"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("endsWithZero0"), SqlTypes.STRING)
        .build();

    final ColumnAliasGenerator generator = ColumnNames.columnAliasGenerator(Stream.of(schema));

    final DereferenceExpression fieldWithNameClash =
        new DereferenceExpression(LOCATION, STRUCT_COLUMN, "12345");

    // When:
    final ColumnName result = generator.uniqueAliasFor(fieldWithNameClash);

    // Then:
    assertThat(result, is(ColumnName.of("12345_1")));
  }

  @Test
  public void shouldEnsureGeneratedAliasesAreCaseSensitive() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("ksql_COL_2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("someField"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("SOMEFIELD_1"), SqlTypes.STRING)
        .build();

    final ColumnAliasGenerator generator = ColumnNames.columnAliasGenerator(Stream.of(schema));

    // When:
    final ColumnName result1 = generator
        .uniqueAliasFor(new DereferenceExpression(LOCATION, STRUCT_COLUMN, "KSQL_COL"));

    final ColumnName result2 = generator
        .uniqueAliasFor(new DereferenceExpression(LOCATION, STRUCT_COLUMN, "someField"));

    // Then:
    assertThat(result1, is(ColumnName.of("KSQL_COL_2")));
    assertThat(result2, is(ColumnName.of("someField_1")));
  }

  @Test
  public void shouldDefaultToRowKeySyntheticJoinColumn() {
    // When:
    final ColumnName columnName = ColumnNames.generateSyntheticJoinKey(Stream.of());

    // Then:
    assertThat(columnName, is(ColumnName.of("ROWKEY")));
  }

  @Test
  public void shouldIncrementSyntheticJoinColumnOnClashes() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("ROWKEY_1"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("ROWKEY_2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("someField"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ROWKEY_3"), SqlTypes.STRING)
        .build();

    // When:
    final ColumnName columnName = ColumnNames.generateSyntheticJoinKey(Stream.of(schema));

    // Then:
    assertThat(columnName, is(ColumnName.of("ROWKEY_4")));
  }

  @Test
  public void shouldDetectPossibleSyntheticJoinColumns() {
    assertThat(ColumnNames.maybeSyntheticJoinKey(ColumnName.of("ROWKEY")), is(true));
    assertThat(ColumnNames.maybeSyntheticJoinKey(ColumnName.of("ROWKEY_0")), is(true));
    assertThat(ColumnNames.maybeSyntheticJoinKey(ColumnName.of("ROWKEY_1")), is(true));

    assertThat(ColumnNames.maybeSyntheticJoinKey(ColumnName.of("Rowkey_2")), is(false));
    assertThat(ColumnNames.maybeSyntheticJoinKey(ColumnName.of("other_2")), is(false));
    assertThat(ColumnNames.maybeSyntheticJoinKey(ColumnName.of("NotROWKEY_2")), is(false));
  }
}