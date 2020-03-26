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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class InsertValuesTest {

  @Test
  public void shouldImplementEqualsHashcode() {
    new EqualsTester()
        .addEqualityGroup(
            new InsertValues(QualifiedName.of("a"), ImmutableList.of(), ImmutableList.of(new NullLiteral())),
            new InsertValues(QualifiedName.of("a"), ImmutableList.of(), ImmutableList.of(new NullLiteral())))
        .addEqualityGroup(new InsertValues(
            QualifiedName.of("diff"), ImmutableList.of(), ImmutableList.of(new StringLiteral("b"))))
        .addEqualityGroup(new InsertValues(
            QualifiedName.of("a"), ImmutableList.of("diff"), ImmutableList.of(new StringLiteral("b"))))
        .addEqualityGroup(new InsertValues(
            QualifiedName.of("a"), ImmutableList.of(), ImmutableList.of(new StringLiteral("diff"))))
        .testEquals();
  }

  @Test
  public void shouldThrowIfEmptyValues() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> new InsertValues(
            QualifiedName.of("a"),
            ImmutableList.of("col1"),
            ImmutableList.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected some values for INSERT INTO statement"));
  }

  @Test
  public void shouldThrowIfNonEmptyColumnsValuesDoNotMatch() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> new InsertValues(
            QualifiedName.of("a"),
            ImmutableList.of("col1"),
            ImmutableList.of(new StringLiteral("val1"), new StringLiteral("val2")))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected number columns and values to match"));
  }

}