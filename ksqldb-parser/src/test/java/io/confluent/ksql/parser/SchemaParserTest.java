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
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.TableElement;
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

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints HEADERS_CONSTRAINT =
      new ColumnConstraints.Builder().headers().build();

  private static final ColumnConstraints HEADER_KEY1_CONSTRAINT =
      new ColumnConstraints.Builder().header("k1").build();

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
        new TableElement(FOO, new Type(SqlTypes.INTEGER)),
        new TableElement(BAR, new Type(SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING
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
        new TableElement(ColumnName.of("K"), new Type(SqlTypes.STRING), KEY_CONSTRAINT),
        new TableElement(BAR, new Type(SqlTypes.INTEGER))
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
        new TableElement(ColumnName.of("K"), new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT),
        new TableElement(BAR, new Type(SqlTypes.INTEGER))
    ));
  }

  @Test
  public void shouldParseValidSchemaWithHeaderField() {
    // Given:
    final String schema = "K STRING HEADERS, bar INT";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(ColumnName.of("K"), new Type(SqlTypes.STRING), HEADERS_CONSTRAINT),
        new TableElement(BAR, new Type(SqlTypes.INTEGER))
    ));
  }

  @Test
  public void shouldParseValidSchemaWithSingleHeaderKeyField() {
    // Given:
    final String schema = "K STRING HEADER('k1'), bar INT";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(ColumnName.of("K"), new Type(SqlTypes.STRING), HEADER_KEY1_CONSTRAINT),
        new TableElement(BAR, new Type(SqlTypes.INTEGER))
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
        new TableElement(ColumnName.of("END"), new Type(SqlTypes.STRING))
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
        new TableElement(ColumnName.of("End"), new Type(SqlTypes.STRING))
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