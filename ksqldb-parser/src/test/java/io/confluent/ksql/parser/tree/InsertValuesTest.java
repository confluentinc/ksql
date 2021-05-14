/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

public class InsertValuesTest {

  private static final SourceName SOME_NAME = SourceName.of("bob");

  @Test
  public void shouldImplementEqualsHashcode() {
    new EqualsTester()
        .addEqualityGroup(
            new InsertValues(SOME_NAME, ImmutableList.of(), ImmutableList.of(new NullLiteral())),
            new InsertValues(SOME_NAME, ImmutableList.of(), ImmutableList.of(new NullLiteral())))
        .addEqualityGroup(new InsertValues(
            SourceName.of("diff"), ImmutableList.of(), ImmutableList.of(new StringLiteral("b"))))
        .addEqualityGroup(new InsertValues(
            SOME_NAME, ImmutableList.of(ColumnName.of("diff")), ImmutableList.of(new StringLiteral("b"))))
        .addEqualityGroup(new InsertValues(
            SOME_NAME, ImmutableList.of(), ImmutableList.of(new StringLiteral("diff"))))
        .testEquals();
  }

  @Test
  public void shouldThrowIfEmptyValues() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () ->  new InsertValues(
            SOME_NAME,
            ImmutableList.of(ColumnName.of("col1")),
            ImmutableList.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected some values for INSERT INTO statement"
    ));
  }

  @Test
  public void shouldThrowIfNonEmptyColumnsValuesDoNotMatch() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () ->  new InsertValues(
            SOME_NAME,
            ImmutableList.of(ColumnName.of("col1")),
            ImmutableList.of(new StringLiteral("val1"), new StringLiteral("val2")))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected number columns and values to match"
    ));
  }

}