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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.ColumnConstraints.Builder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Test;

public class TableElementsTest {

  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final ColumnConstraints HEADERS_CONSTRAINT =
      new ColumnConstraints.Builder().headers().build();

  private static final Type INT_TYPE = new Type(SqlTypes.INTEGER);
  private static final Type STRING_TYPE = new Type(SqlTypes.STRING);
  private static final Type BYTES_TYPE = new Type(SqlTypes.BYTES);

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    final List<TableElement> someElements = ImmutableList.of(
        tableElement("bob", INT_TYPE)
    );

    new EqualsTester()
        .addEqualityGroup(TableElements.of(someElements), TableElements.of(someElements))
        .addEqualityGroup(TableElements.of())
        .testEquals();
  }

  @Test
  public void shouldSupportKeyColumnsAfterValues() {
    // Given:
    final TableElement key = tableElement( "key", STRING_TYPE, KEY_CONSTRAINT);
    final TableElement value = tableElement("v0", INT_TYPE);
    final List<TableElement> elements = ImmutableList.of(value, key);

    // When:
    final TableElements result = TableElements.of(elements);

    // Then:
    assertThat(result, contains(value, key));
  }

  @Test
  public void shouldThrowOnDuplicateKeyColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement( "k0", STRING_TYPE, KEY_CONSTRAINT),
        tableElement("k0", STRING_TYPE, KEY_CONSTRAINT),
        tableElement("k1", STRING_TYPE, KEY_CONSTRAINT),
        tableElement("k1", STRING_TYPE, PRIMARY_KEY_CONSTRAINT)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> TableElements.of(elements)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Duplicate column names:"));
    assertThat(e.getMessage(), containsString(
        "k0"));
    assertThat(e.getMessage(), containsString(
        "k1"));
  }

  @Test
  public void shouldThrowOnDuplicateHeaderColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement("k0", STRING_TYPE, HEADERS_CONSTRAINT),
        tableElement("k0", STRING_TYPE, HEADERS_CONSTRAINT),
        tableElement("k1", STRING_TYPE, HEADERS_CONSTRAINT),
        tableElement("k1", STRING_TYPE, PRIMARY_KEY_CONSTRAINT)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> TableElements.of(elements)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Duplicate column names:"));
    assertThat(e.getMessage(), containsString(
        "k0"));
    assertThat(e.getMessage(), containsString(
        "k1"));
  }

  @Test
  public void shouldThrowOnDuplicateValueColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement("v0", INT_TYPE),
        tableElement("v0", INT_TYPE),
        tableElement("v1", INT_TYPE),
        tableElement("v1", INT_TYPE)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> TableElements.of(elements)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Duplicate column names:"));
    assertThat(e.getMessage(), containsString(
        "v0"));
    assertThat(e.getMessage(), containsString(
        "v1"));
  }

  @Test
  public void shouldThrowOnDuplicateKeyValueColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement("v0", INT_TYPE, KEY_CONSTRAINT),
        tableElement("v0", INT_TYPE),
        tableElement("v1", INT_TYPE, PRIMARY_KEY_CONSTRAINT),
        tableElement("v1", INT_TYPE)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> TableElements.of(elements)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Duplicate column names:"));
    assertThat(e.getMessage(), containsString(
        "v0"));
    assertThat(e.getMessage(), containsString(
        "v1"));
  }

  @Test
  public void shouldNotThrowOnNoKeyElements() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement("v0", new Type(SqlTypes.INTEGER))
    );

    // When:
    TableElements.of(elements);

    // Then: did not throw.
  }

  @Test
  public void shouldIterateElements() {
    // Given:
    final TableElement te1 = tableElement("k0", STRING_TYPE, KEY_CONSTRAINT);
    final TableElement te2 = tableElement("v0", INT_TYPE);

    // When:
    final Iterable<TableElement> iterable = TableElements.of(ImmutableList.of(te1, te2));

    // Then:
    assertThat(iterable, contains(te1, te2));
  }

  @Test
  public void shouldStreamElements() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement("k0", STRING_TYPE, KEY_CONSTRAINT),
        tableElement("v0", INT_TYPE)
    );

    final TableElements tableElements = TableElements.of(elements);

    // When:
    final List<TableElement> result = tableElements.stream()
        .collect(Collectors.toList());

    // Then:
    assertThat(result, is(elements));
  }

  @Test
  public void shouldToString() {
    // Given:
    final TableElement element0 = tableElement("k0", STRING_TYPE, KEY_CONSTRAINT);
    final TableElement element1 = tableElement("v0", INT_TYPE);

    final TableElements tableElements = TableElements.of(element0, element1);

    // When:
    final String string = tableElements.toString();

    // Then:
    assertThat(string, is("[" + element0 + ", " + element1 + "]"));
  }

  @Test
  public void shouldThrowWhenBuildLogicalSchemaIfNoElements() {
    // Given:
    final TableElements tableElements = TableElements.of();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> tableElements.toLogicalSchema()
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "No columns supplied."));
  }

  @Test
  public void shouldBuildLogicalSchemaWithOutKey() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement("v0", INT_TYPE)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema();

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithWithKey() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement("v0", INT_TYPE),
        tableElement("k0", INT_TYPE, KEY_CONSTRAINT)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema();

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithWithHeaders() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement("v0", INT_TYPE),
        tableElement("h0", new Type(SqlArray.of(
            SqlStruct.builder()
                .field("KEY", SqlTypes.STRING)
                .field("VALUE", SqlTypes.BYTES).build())), HEADERS_CONSTRAINT)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema();

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .headerColumn(ColumnName.of("h0"), Optional.empty())
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithWithExtractedHeader() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement("v0", INT_TYPE),
        tableElement("h0", BYTES_TYPE, new Builder().header("key").build())
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema();

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .headerColumn(ColumnName.of("h0"), Optional.of("key"))
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithWithPrimaryKey() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement("v0", INT_TYPE),
        tableElement("k0", INT_TYPE, PRIMARY_KEY_CONSTRAINT)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema();

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithKeyHeaderAndValueColumnsInterleaved() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement("v0", INT_TYPE),
        tableElement("k0", INT_TYPE, KEY_CONSTRAINT),
        tableElement("v1", STRING_TYPE),
        tableElement("h1", BYTES_TYPE, new Builder().header("key").build()),
        tableElement("k1", INT_TYPE, KEY_CONSTRAINT)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema();

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .headerColumn(ColumnName.of("h1"), Optional.of("key"))
        .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
        .build()
    ));
  }

  private static TableElement tableElement(
      final String name,
      final Type type
  ) {
    return tableElement(name, type, ColumnConstraints.NO_COLUMN_CONSTRAINTS);
  }

  private static TableElement tableElement(
      final String name,
      final Type type,
      final ColumnConstraints constraints
  ) {
    final TableElement te = mock(TableElement.class, name);
    when(te.getName()).thenReturn(ColumnName.of(name));
    when(te.getType()).thenReturn(type);
    when(te.getConstraints()).thenReturn(constraints);
    return te;
  }
}