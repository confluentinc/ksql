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

import static io.confluent.ksql.parser.tree.TableElement.Namespace.KEY;
import static io.confluent.ksql.parser.tree.TableElement.Namespace.VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableElementsTest {

  private static final Type INT_TYPE = new Type(SqlTypes.INTEGER);
  private static final Type STRING_TYPE = new Type(SqlTypes.STRING);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    final List<TableElement> someElements = ImmutableList.of(
        tableElement(VALUE, "bob", INT_TYPE)
    );

    new EqualsTester()
        .addEqualityGroup(TableElements.of(someElements), TableElements.of(someElements))
        .addEqualityGroup(TableElements.of())
        .testEquals();
  }

  @Test
  public void shouldSupportKeyColumnsAfterValues() {
    // Given:
    final TableElement key = tableElement(KEY, "key", STRING_TYPE);
    final TableElement value = tableElement(VALUE, "v0", INT_TYPE);
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
        tableElement(KEY, "k0", STRING_TYPE),
        tableElement(KEY, "k0", STRING_TYPE),
        tableElement(KEY, "k1", STRING_TYPE),
        tableElement(KEY, "k1", STRING_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate column names:");
    expectedException.expectMessage("k0");
    expectedException.expectMessage("k1");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldThrowOnDuplicateValueColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(VALUE, "v0", INT_TYPE),
        tableElement(VALUE, "v0", INT_TYPE),
        tableElement(VALUE, "v1", INT_TYPE),
        tableElement(VALUE, "v1", INT_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate column names:");
    expectedException.expectMessage("v0");
    expectedException.expectMessage("v1");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldThrowOnDuplicateKeyValueColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(KEY, "v0", INT_TYPE),
        tableElement(VALUE, "v0", INT_TYPE),
        tableElement(KEY, "v1", INT_TYPE),
        tableElement(VALUE, "v1", INT_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate column names:");
    expectedException.expectMessage("v0");
    expectedException.expectMessage("v1");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldNotThrowOnNoKeyElements() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(VALUE, "v0", new Type(SqlTypes.INTEGER))
    );

    // When:
    TableElements.of(elements);

    // Then: did not throw.
  }

  @Test
  public void shouldIterateElements() {
    // Given:
    final TableElement te1 = tableElement(KEY, "k0", STRING_TYPE);
    final TableElement te2 = tableElement(VALUE, "v0", INT_TYPE);

    // When:
    final Iterable<TableElement> iterable = TableElements.of(ImmutableList.of(te1, te2));

    // Then:
    assertThat(iterable, contains(te1, te2));
  }

  @Test
  public void shouldStreamElements() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(KEY, "k0", STRING_TYPE),
        tableElement(VALUE, "v0", INT_TYPE)
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
    final TableElement element0 = tableElement(KEY, "k0", STRING_TYPE);
    final TableElement element1 = tableElement(VALUE, "v0", INT_TYPE);

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

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("No columns supplied.");

    // When:
    tableElements.toLogicalSchema(true);
  }

  @Test
  public void shouldBuildLogicalSchemaWithImplicits() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement(VALUE, "v0", INT_TYPE)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema(true);

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithOutImplicits() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement(VALUE, "v0", INT_TYPE)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema(false);

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .noImplicitColumns()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build()
    ));
  }

  @Test
  public void shouldBuildLogicalSchemaWithKeyAndValueColumnsInterleaved() {
    // Given:
    final TableElements tableElements = TableElements.of(
        tableElement(VALUE, "v0", INT_TYPE),
        tableElement(KEY, "k0", INT_TYPE),
        tableElement(VALUE, "v1", STRING_TYPE),
        tableElement(KEY, "k1", INT_TYPE)
    );

    // When:
    final LogicalSchema schema = tableElements.toLogicalSchema(false);

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .noImplicitColumns()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
        .build()
    ));
  }

  private static TableElement tableElement(
      final Namespace namespace,
      final String name,
      final Type type
  ) {
    final TableElement te = mock(TableElement.class, name);
    when(te.getName()).thenReturn(ColumnName.of(name));
    when(te.getType()).thenReturn(type);
    when(te.getNamespace()).thenReturn(namespace);
    return te;
  }
}