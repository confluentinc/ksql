/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaParserTest {

  private static final ColumnName FOO = ColumnName.of("FOO");
  private static final ColumnName BAR = ColumnName.of("BAR");

  @Mock
  private TypeRegistry typeRegistry;
  private SchemaParser parser;

  @Before
  public void setUp() {
    parser = new SchemaParser(typeRegistry);
  }

  @Test
  public void shouldParseValidSchema() {
    // Given:
    final String schema = "foo INTEGER, bar MAP<VARCHAR, VARCHAR>";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(Namespace.VALUE, FOO, new Type(SqlTypes.INTEGER)),
        new TableElement(Namespace.VALUE, BAR, new Type(SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING
        )))
    ));
  }

  @Test
  public void shouldParseValidSchemaWithKeyField() {
    // Given:
    final String schema = "K STRING KEY, bar INT";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(Namespace.KEY, ColumnName.of("K"), new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, BAR, new Type(SqlTypes.INTEGER))
    ));
  }

  @Test
  public void shouldParseValidSchemaWithPrimaryKeyField() {
    // Given:
    final String schema = "K STRING PRIMARY KEY, bar INT";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(Namespace.PRIMARY_KEY, ColumnName.of("K"), new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, BAR, new Type(SqlTypes.INTEGER))
    ));
  }

  @Test
  public void shouldParseQuotedSchema() {
    // Given:
    final String schema = "`END` VARCHAR";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, hasItem(
        new TableElement(Namespace.VALUE, ColumnName.of("END"), new Type(SqlTypes.STRING))
    ));
  }

  @Test
  public void shouldParseQuotedMixedCase() {
    // Given:
    final String schema = "`End` VARCHAR";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, hasItem(
        new TableElement(Namespace.VALUE, ColumnName.of("End"), new Type(SqlTypes.STRING))
    ));
  }

  @Test
  public void shouldParseEmptySchema() {
    // Given:
    final String schema = " \t\n\r";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(Iterables.isEmpty(elements), is(true));
  }

  @Test
  public void shouldThrowOnInvalidSchema() {
    // Given:
    final String schema = "foo-bar INTEGER";

    // Expect:
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> parser.parse(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error parsing schema \"foo-bar INTEGER\" at 1:4: extraneous input '-' "));
  }

  @Test
  public void shouldThrowOnReservedWord() {
    // Given:
    final String schema = "CREATE INTEGER";

    // Expect:
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> parser.parse(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error parsing schema \"CREATE INTEGER\" at 1:1: extraneous input 'CREATE' "));
  }
}