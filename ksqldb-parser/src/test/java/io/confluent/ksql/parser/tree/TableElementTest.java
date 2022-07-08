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

import static io.confluent.ksql.parser.tree.ColumnConstraints.NO_COLUMN_CONSTRAINTS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class TableElementTest {

  private static final Optional<NodeLocation> A_LOCATION =
      Optional.of(new NodeLocation(2, 4));

  private static final ColumnName NAME = ColumnName.of("name");

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints HEADERS_CONSTRAINT =
      new ColumnConstraints.Builder().headers().build();

  private static final ColumnConstraints HEADER_SINGLE_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().header("k1").build();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableElement(A_LOCATION, NAME, new Type(SqlTypes.STRING), NO_COLUMN_CONSTRAINTS),
            new TableElement(NAME, new Type(SqlTypes.STRING), NO_COLUMN_CONSTRAINTS)
        )
        .addEqualityGroup(
            new TableElement(ColumnName.of("different"), new Type(SqlTypes.STRING),
                NO_COLUMN_CONSTRAINTS)
        )
        .addEqualityGroup(
            new TableElement(NAME, new Type(SqlTypes.INTEGER), NO_COLUMN_CONSTRAINTS)
        )
        .addEqualityGroup(
            new TableElement(NAME, new Type(SqlTypes.STRING), KEY_CONSTRAINT)
        )
        .addEqualityGroup(
            new TableElement(NAME, new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT)
        )
        .addEqualityGroup(
            new TableElement(NAME, new Type(SqlTypes.STRING), HEADERS_CONSTRAINT)
        )
        .addEqualityGroup(
            new TableElement(NAME, new Type(SqlTypes.STRING), HEADER_SINGLE_KEY_CONSTRAINT)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    // Given:
    final TableElement element =
        new TableElement(NAME, new Type(SqlTypes.STRING), NO_COLUMN_CONSTRAINTS);

    // Then:
    assertThat(element.getName(), is(NAME));
  }

  @Test
  public void shouldReturnType() {
    // Given:
    final TableElement element = new TableElement(NAME, new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getType(), is(new Type(SqlTypes.STRING)));
  }

  @Test
  public void shouldReturnEmptyConstraints() {
    // Given:
    final TableElement valueElement = new TableElement(NAME, new Type(SqlTypes.STRING));

    // Then:
    assertThat(valueElement.getConstraints(), is(NO_COLUMN_CONSTRAINTS));
  }

  @Test
  public void shouldReturnPrimaryKey() {
    // Given:
    final TableElement valueElement = new TableElement(NAME, new Type(SqlTypes.STRING),
        PRIMARY_KEY_CONSTRAINT);

    // Then:
    assertThat(valueElement.getConstraints(), is(PRIMARY_KEY_CONSTRAINT));
  }

  @Test
  public void shouldReturnKeyConstraint() {
    // Given:
    final TableElement valueElement = new TableElement(NAME, new Type(SqlTypes.STRING),
        KEY_CONSTRAINT);

    // Then:
    assertThat(valueElement.getConstraints(), is(KEY_CONSTRAINT));
  }

  @Test
  public void shouldReturnHeadersConstraint() {
    // Given:
    final TableElement valueElement = new TableElement(NAME, new Type(SqlTypes.STRING),
        HEADERS_CONSTRAINT);

    // Then:
    assertThat(valueElement.getConstraints(), is(HEADERS_CONSTRAINT));
  }

  @Test
  public void shouldReturnSingleHeaderKeyConstraint() {
    // Given:
    final TableElement valueElement = new TableElement(NAME, new Type(SqlTypes.STRING),
        HEADER_SINGLE_KEY_CONSTRAINT);

    // Then:
    assertThat(valueElement.getConstraints(), is(HEADER_SINGLE_KEY_CONSTRAINT));
  }
}