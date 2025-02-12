/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.attr;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udaf.attr.Attr.Impl;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class AttrTest {

  // NOTE: most of the test coverage is in attr.json, this file only
  // tests the merge and undo methods, which is otherwise not capable
  // of being covered easily by QTT tests (such as different schemas
  // and valid behaviors for aggregation/mapping)

  private static final Impl<Integer> ATTR = new Impl<>();

  static {
    ATTR.initializeTypeArguments(ImmutableList.of(SqlArgument.of(SqlTypes.INTEGER)));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final List<Struct> one = ImmutableList.of(build(1, 2), build(2, 1));
    final List<Struct> two = ImmutableList.of(build(1, 2), build(3, 1));

    // When:
    final List<Struct> out = ATTR.merge(one, two);

    // Then:
    assertThat(out, hasItems(
        build(1, 4), build(2, 1), build(3, 1)
    ));
  }

  @Test
  public void shouldMergeWithNulls() {
    // Given:
    final List<Struct> one = ImmutableList.of(build(null, 2));
    final List<Struct> two = ImmutableList.of(build(1, 2));

    // When:
    final List<Struct> out = ATTR.merge(one, two);

    // Then:
    assertThat(out, hasItems(build(null, 2), build(1, 2)));
  }

  @Test
  public void shouldMergeWithEmptyList() {
    // Given:
    final List<Struct> one = ImmutableList.of();
    final List<Struct> two = ImmutableList.of(build(1, 2));

    // When:
    final List<Struct> out = ATTR.merge(one, two);

    // Then:
    assertThat(out, hasItems(build(1, 2)));
  }

  @Test
  public void shouldUndoExistingValue() {
    // Given:
    final List<Struct> agg = ImmutableList.of(build(1, 2), build(2, 1));

    // When:
    final List<Struct> undo = ATTR.undo(1, agg);

    // Then:
    assertThat(undo, hasItem(build(1, 1)));
  }

  @Test
  public void shouldUndoMissingValue() {
    // Given:
    final List<Struct> agg = ImmutableList.of(build(2, 1));

    // When:
    final List<Struct> undo = ATTR.undo(1, agg);

    // Then:
    assertThat(undo, hasSize(1));
  }

  @Test
  public void shouldNotUndoBelowZero() {
    // Given:
    final List<Struct> agg = ImmutableList.of(build(1, 0));

    // When:
    final List<Struct> undo = ATTR.undo(1, agg);

    // Then:
    assertThat(undo, hasItem(build(1, 0)));
  }

  private Struct build(final Integer value, final Integer count) {
    return new Struct(ATTR.entrySchema)
        .put(Impl.VALUE, value)
        .put(Impl.COUNT, count);
  }

}