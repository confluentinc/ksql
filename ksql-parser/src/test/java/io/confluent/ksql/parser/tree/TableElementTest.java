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

import static io.confluent.ksql.util.SchemaUtil.ROWKEY_NAME;
import static io.confluent.ksql.util.SchemaUtil.ROWTIME_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableElementTest {

  private static final Optional<NodeLocation> A_LOCATION =
      Optional.of(new NodeLocation(2, 4));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableElement(A_LOCATION, "name", new Type(SqlTypes.STRING)),
            new TableElement("name", new Type(SqlTypes.STRING))
        )
        .addEqualityGroup(
            new TableElement("different", new Type(SqlTypes.STRING))
        )
        .addEqualityGroup(
            new TableElement("name", new Type(SqlTypes.INTEGER))
        )
       .testEquals();
  }

  @Test
  public void shouldThrowOnRowTimeValueColumn() {
    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 2:6: 'ROWTIME' is a reserved field name.");

    // When:
    new TableElement(A_LOCATION, ROWTIME_NAME, new Type(SqlTypes.BIGINT));
  }

  @Test
  public void shouldThrowOnRowKeyValueColumn() {
    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage(
        "line 2:6: 'ROWKEY' is a reserved field name.");

    // When:
    new TableElement(A_LOCATION, ROWKEY_NAME, new Type(SqlTypes.STRING));
  }

  @Test
  public void shouldReturnName() {
    // Given:
    final TableElement element = new TableElement("name", new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getName(), is("name"));
  }

  @Test
  public void shouldReturnType() {
    // Given:
    final TableElement element = new TableElement("name", new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getType(), is(new Type(SqlTypes.STRING)));
  }
}