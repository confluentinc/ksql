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
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.parser.NodeLocation;
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
            new TableElement(A_LOCATION, VALUE, "name", new Type(SqlTypes.STRING)),
            new TableElement(VALUE, "name", new Type(SqlTypes.STRING))
        )
        .addEqualityGroup(
            new TableElement(VALUE, "different", new Type(SqlTypes.STRING))
        )
        .addEqualityGroup(
            new TableElement(VALUE, "name", new Type(SqlTypes.INTEGER))
        )
        .addEqualityGroup(
            new TableElement(KEY, "ROWKEY", new Type(SqlTypes.STRING))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    // Given:
    final TableElement element =
        new TableElement(VALUE, "name", new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getName(), is("name"));
  }

  @Test
  public void shouldReturnType() {
    // Given:
    final TableElement element = new TableElement(VALUE, "name", new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getType(), is(new Type(SqlTypes.STRING)));
  }

  @Test
  public void shouldReturnNamespace() {
    // Given:
    final TableElement valueElement = new TableElement(VALUE, "name", new Type(SqlTypes.STRING));

    // Then:
    assertThat(valueElement.getNamespace(), is(VALUE));
  }
}