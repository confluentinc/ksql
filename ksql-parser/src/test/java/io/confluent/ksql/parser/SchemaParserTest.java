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
import static org.hamcrest.Matchers.empty;

import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SchemaParserTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldParseValidSchema() {
    // Given:
    final String schema = "foo INTEGER, bar MAP<VARCHAR, VARCHAR>";

    // When:
    final List<TableElement> elements = SchemaParser.parse(schema);

    // Then:
    assertThat(elements.size(), is(2));
    assertThat(
        elements.get(0),
        is(new TableElement("FOO", PrimitiveType.of(SqlType.INTEGER))));
    assertThat(
        elements.get(1),
        is(new TableElement("BAR", Map.of(PrimitiveType.of(SqlType.STRING)))));
  }

  @Test
  public void shouldParseQuotedSchema() {
    // Given:
    final String schema = "`END` VARCHAR";

    // When:
    final List<TableElement> elements = SchemaParser.parse(schema);

    // Then:
    assertThat(elements.size(), is(1));
    assertThat(elements.get(0), is(new TableElement("END", PrimitiveType.of(SqlType.STRING))));
  }

  @Test
  public void shouldParseEmptySchema() {
    // Given:
    final String schema = "";

    // When:
    final List<TableElement> elements = SchemaParser.parse(schema);

    // Then:
    assertThat(elements, empty());
  }

  @Test
  public void shouldThrowOnInvalidSchema() {
    // Given:
    final String schema = "foo-bar INTEGER";

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Error parsing schema \"foo-bar INTEGER\" at 1:4: extraneous input '-' ");

    // When:
    SchemaParser.parse(schema);
  }

  @Test
  public void shouldThrowOnReservedWord() {
    // Given:
    final String schema = "CREATE INTEGER";

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Error parsing schema \"CREATE INTEGER\" at 1:1: extraneous input 'CREATE' ");

    // When:
    SchemaParser.parse(schema);
  }

}