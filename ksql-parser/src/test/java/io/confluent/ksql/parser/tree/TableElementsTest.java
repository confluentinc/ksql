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
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableElementsTest {

  private static final PrimitiveType SOME_TYPE = PrimitiveType.of("INT");
  private static final PrimitiveType STRING_TYPE = PrimitiveType.of("STRING");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    final List<TableElement> someElements = ImmutableList.of(
        tableElement(VALUE, "bob", SOME_TYPE)
    );

    new EqualsTester()
        .addEqualityGroup(TableElements.of(someElements), TableElements.of(someElements))
        .addEqualityGroup(TableElements.of())
        .testEquals();
  }

  @Test
  public void shouldThrowOnOutOfOrderKeyColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(VALUE, "v0", SOME_TYPE),
        tableElement(KEY, "key", STRING_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("KEY column declared after VALUE column: key");

    // When:
    TableElements.of(elements);
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
    expectedException.expectMessage("Duplicate KEY column names:");
    expectedException.expectMessage("k0");
    expectedException.expectMessage("k1");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldThrowOnDuplicateValueColumns() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(VALUE, "v0", SOME_TYPE),
        tableElement(VALUE, "v0", SOME_TYPE),
        tableElement(VALUE, "v1", SOME_TYPE),
        tableElement(VALUE, "v1", SOME_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Duplicate non-KEY column names:");
    expectedException.expectMessage("v0");
    expectedException.expectMessage("v1");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldThrowIfMoreThatOneKeyColumn() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(KEY, "k0", STRING_TYPE),
        tableElement(KEY, "k1", STRING_TYPE),
        tableElement(VALUE, "v0", SOME_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("KSQL does not yet support multiple KEY columns");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldThrowIfKeyColumnNotString() {
    // Given:
    final List<TableElement> elements = ImmutableList.of(
        tableElement(KEY, "k0", PrimitiveType.of("INT")),
        tableElement(VALUE, "v0", SOME_TYPE)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("KEY columns must be of type STRING: k0");

    // When:
    TableElements.of(elements);
  }

  @Test
  public void shouldIterateElements() {
    // Given:
    final TableElement te1 = tableElement(KEY, "k0", STRING_TYPE);
    final TableElement te2 = tableElement(VALUE, "v0", SOME_TYPE);

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
        tableElement(VALUE, "v0", SOME_TYPE)
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
    final TableElement element1 = tableElement(VALUE, "v0", SOME_TYPE);

    final TableElements tableElements = TableElements.of(element0, element1);

    // When:
    final String string = tableElements.toString();

    // Then:
    assertThat(string, is("[" + element0 + ", " + element1 + "]"));
  }

  private static TableElement tableElement(
      final Namespace namespace,
      final String name,
      final Type type
  ) {
    final TableElement te = mock(TableElement.class, name);
    when(te.getName()).thenReturn(name);
    when(te.getType()).thenReturn(type);
    when(te.getNamespace()).thenReturn(namespace);
    return te;
  }
}