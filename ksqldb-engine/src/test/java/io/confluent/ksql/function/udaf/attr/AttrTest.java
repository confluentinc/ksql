/*
 * Copyright 2020 Confluent Inc.
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
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udaf.attr.Attr.Impl;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class AttrTest {

  // NOTE: most of the test coverage is in attr.json, this file only
  // tests the merge method, which is otherwise not capable of being
  // covered easily by QTT tests (such as different schemas and valid
  // behaviors for aggregation/mapping)

  private Schema schema;
  private Impl<Integer> attr;

  @Before
  public void setUp() {
    attr = new Impl<>();
    attr.initializeTypeArguments(ImmutableList.of(SqlArgument.of(SqlTypes.INTEGER)));
    schema = attr.schema;
  }

  @Test
  public void shouldMergeTwoIdentical() {
    // Given:
    final Struct val1 = build(true, true);
    final Struct val2 = build(true, true);

    // When:
    final Struct merge = attr.merge(val1, val2);

    // Then:
    assertThat(merge, equalTo(val1));
  }

  @Test
  public void shouldChooseInitializedIfOnlyFirstIsInit() {
    // Given:
    final Struct val1 = build(true, true);
    final Struct val2 = build(false, true);

    // When:
    final Struct merge = attr.merge(val1, val2);

    // Then:
    assertThat(merge, equalTo(val1));
  }

  @Test
  public void shouldChooseInitializedIfOnlySecondIsInit() {
    // Given:
    final Struct val1 = build(false, true);
    final Struct val2 = build(true, true);

    // When:
    final Struct merge = attr.merge(val1, val2);

    // Then:
    assertThat(merge, equalTo(val2));
  }

  @Test
  public void shouldSetInvalidIfFirstIsInvalid() {
    // Given:
    final Struct val1 = build(true, false);
    final Struct val2 = build(true, true);

    // When:
    final Struct merge = attr.merge(val1, val2);

    // Then:
    assertThat(merge.getBoolean(Impl.VALID), equalTo(false));
  }

  @Test
  public void shouldSetInvalidIfSecondIsInvalid() {
    // Given:
    final Struct val1 = build(true, true);
    final Struct val2 = build(true, false);

    // When:
    final Struct merge = attr.merge(val1, val2);

    // Then:
    assertThat(merge.getBoolean(Impl.VALID), equalTo(false));
  }

  private Struct build(final boolean init, final boolean valid) {
    return new Struct(schema)
        .put(Impl.INIT, init)
        .put(Impl.VALID, valid)
        .put(Impl.VALUE, 1);
  }

}