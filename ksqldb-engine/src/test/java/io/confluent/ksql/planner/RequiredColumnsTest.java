/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.RequiredColumns.Builder;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class RequiredColumnsTest {

  private static final SourceName SRC0 = SourceName.of("SRC0");

  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName COL2 = ColumnName.of("COL2");
  private static final ColumnName COL3 = ColumnName.of("COL3");

  private static final UnqualifiedColumnReferenceExp COL0_REF =
      new UnqualifiedColumnReferenceExp(COL0);

  private static final QualifiedColumnReferenceExp COL1_REF =
      new QualifiedColumnReferenceExp(SRC0, COL1);

  private static final UnqualifiedColumnReferenceExp COL2_REF =
      new UnqualifiedColumnReferenceExp(COL2);

  private static final QualifiedColumnReferenceExp COL3_REF =
      new QualifiedColumnReferenceExp(SRC0, COL3);

  private static final BetweenPredicate EXP0 = new BetweenPredicate(COL1_REF, COL2_REF, COL3_REF);

  private Builder builder;

  @Before
  public void setUp() {
    builder = RequiredColumns.builder();
  }

  @Test
  public void shouldImplementEqualsAndHashCode() {
    new EqualsTester()
        .addEqualityGroup(
            RequiredColumns.builder().add(EXP0).build(),
            RequiredColumns.builder().addAll(ImmutableSet.of(COL1_REF, COL2_REF, COL3_REF)).build()
        )
        .addEqualityGroup(
            RequiredColumns.builder().add(COL0_REF).build()
        )
        .testEquals();
  }

  @Test
  public void shouldAddColRef() {
    // When:
    builder.add(COL0_REF).add(EXP0);

    // Then:
    assertThat(builder.build().get(), is(ImmutableSet.of(COL0_REF, COL1_REF, COL2_REF, COL3_REF)));
  }

  @Test
  public void shouldAddAll() {
    // When:
    builder.addAll(ImmutableSet.of(COL0_REF, COL1_REF, EXP0));

    // Then:
    assertThat(builder.build().get(), is(ImmutableSet.of(COL0_REF, COL1_REF, COL2_REF, COL3_REF)));
  }

  @Test
  public void shouldRemove() {
    // Given:
    builder.addAll(ImmutableSet.of(COL0_REF, COL1_REF, COL2_REF));

    // When:
    builder.remove(COL1_REF);

    // Then:
    assertThat(builder.build().get(), is(ImmutableSet.of(COL0_REF, COL2_REF)));
  }

  @Test
  public void shouldRemoveAll() {
    // Given:
    builder.addAll(ImmutableSet.of(COL0_REF, COL1_REF, COL2_REF));

    // When:
    builder.removeAll(ImmutableSet.of(COL0_REF, COL2_REF));

    // Then:
    assertThat(builder.build().get(), is(ImmutableSet.of(COL1_REF)));
  }

  @Test
  public void shouldBeImmutable() {
    // Given:
    final RequiredColumns requiredColumns = builder.add(COL0_REF).add(COL1_REF).build();
    final Builder builder2 = requiredColumns.asBuilder();

    // When:
    builder.remove(COL0_REF);
    builder2.remove(COL1_REF);

    // Then:
    assertThat(requiredColumns.get(), is(ImmutableSet.of(COL0_REF, COL1_REF)));
  }
}