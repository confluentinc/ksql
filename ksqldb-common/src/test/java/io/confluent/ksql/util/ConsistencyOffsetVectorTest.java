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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableMap;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class ConsistencyOffsetVectorTest {
  private static final String TEST_TOPIC = "testTopic";
  private static final String TEST_TOPIC1 = "testTopic1";
  private static final String TEST_TOPIC2 = "testTopic2";

  @Test
  public void shouldSerializeAndDeserialize() {
    // Given:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector();
    offsetVector
        .withComponent(TEST_TOPIC1, 1, 1L)
        .withComponent(TEST_TOPIC1, 2, 2L)
        .withComponent(TEST_TOPIC1, 3, 3L);

    // When:
    final ConsistencyOffsetVector deserializedCV =
        ConsistencyOffsetVector.deserialize(offsetVector.serialize());

    // Then:
    assertThat(deserializedCV.equals(offsetVector), is(true));
  }

  @Test
  public void shouldUpdateWithLargerOffset() {
    // Given:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector()
        .withComponent(TEST_TOPIC1, 1, 1L);

    // When:
    offsetVector.update(TEST_TOPIC1, 1, 2L);

    // Then:
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC1).get(1), is(2L));
  }

  @Test
  public void shouldNotUpdate() {
    // Given:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector()
        .withComponent(TEST_TOPIC1, 1, 2L);

    // When:
    offsetVector.update(TEST_TOPIC1, 1, 1L);

    // Then:
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC1).get(1), is(2L));
  }

  @Test
  public void shouldUpdateComponentMonotonically() {
    // Given:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector();

    // When:
    offsetVector.withComponent(TEST_TOPIC1, 3, 5L);
    offsetVector.withComponent(TEST_TOPIC1, 3, 4L);

    // Then:
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC1).get(3), equalTo(5L));

    // When:
    offsetVector.withComponent(TEST_TOPIC1, 3, 6L);

    //Then:
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC1).get(3), equalTo(6L));
  }

  @Test
  public void shouldCopy() {
    // Give:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector()
        .withComponent(TEST_TOPIC, 0, 5L)
        .withComponent(TEST_TOPIC2, 0, 5L)
        .withComponent(TEST_TOPIC2, 7, 0L);

    final ConsistencyOffsetVector copy = offsetVector.copy();

    // When:
    // mutate original
    offsetVector.withComponent(TEST_TOPIC, 0, 6L);
    offsetVector.withComponent(TEST_TOPIC1, 8, 1L);
    offsetVector.withComponent(TEST_TOPIC2, 2, 4L);

    // Then:
    // copy has not changed
    assertThat(copy.getTopicOffsets(TEST_TOPIC), equalTo(ImmutableMap.of(0, 5L)));
    assertThat(copy.getTopicOffsets(TEST_TOPIC2), equalTo(ImmutableMap.of(0, 5L, 7, 0L)));

    // original has changed
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC), equalTo(ImmutableMap.of(0, 6L)));
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC1), equalTo(ImmutableMap.of(8, 1L)));
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC2), equalTo(ImmutableMap.of(0, 5L, 7, 0L, 2, 4L)));
  }

  @Test
  public void shouldMatchOnEqual() {
    // Given:
    final ConsistencyOffsetVector offsetVector1 = ConsistencyOffsetVector.emptyVector();
    final ConsistencyOffsetVector offsetVector2 = ConsistencyOffsetVector.emptyVector();
    offsetVector1.withComponent(TEST_TOPIC1, 0, 1);
    offsetVector2.withComponent(TEST_TOPIC1, 0, 1);

    offsetVector1.withComponent(TEST_TOPIC1, 1, 2);
    offsetVector2.withComponent(TEST_TOPIC1, 1, 2);

    offsetVector1.withComponent(TEST_TOPIC1, 2, 1);
    offsetVector2.withComponent(TEST_TOPIC1, 2, 1);

    offsetVector1.withComponent(TEST_TOPIC2, 0, 0);
    offsetVector2.withComponent(TEST_TOPIC2, 0, 0);

    // Then:
    assertEquals(offsetVector1, offsetVector2);
  }

  @Test
  public void shouldNotMatchOnUnEqual() {
    // Given:
    final ConsistencyOffsetVector offsetVector1 = ConsistencyOffsetVector.emptyVector();
    final ConsistencyOffsetVector offsetVector2 = ConsistencyOffsetVector.emptyVector();

    offsetVector1.withComponent(TEST_TOPIC1, 0, 1);
    offsetVector2.withComponent(TEST_TOPIC1, 0, 1);

    offsetVector1.withComponent(TEST_TOPIC1, 1, 2);

    offsetVector1.withComponent(TEST_TOPIC1, 2, 1);
    offsetVector2.withComponent(TEST_TOPIC1, 2, 1);

    offsetVector1.withComponent(TEST_TOPIC2, 0, 0);
    offsetVector2.withComponent(TEST_TOPIC2, 0, 0);

    assertNotEquals(offsetVector1, offsetVector2);
  }

  @Test
  public void shouldMerge() {
    // Given:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector();
    offsetVector.update(TEST_TOPIC1, 0, 1);
    offsetVector.update(TEST_TOPIC1, 1, 2);
    offsetVector.update(TEST_TOPIC1, 2, 1);

    final ConsistencyOffsetVector other = ConsistencyOffsetVector.emptyVector();
    other.update(TEST_TOPIC1, 2, 2);
    other.update(TEST_TOPIC2, 0, 1);

    offsetVector.merge(other);

    final ConsistencyOffsetVector mergedVector = ConsistencyOffsetVector.emptyVector();
    mergedVector.update(TEST_TOPIC1, 0, 1);
    mergedVector.update(TEST_TOPIC1, 1, 2);
    mergedVector.update(TEST_TOPIC1, 2, 2);
    mergedVector.update(TEST_TOPIC2, 0, 1);

    assertThat(offsetVector, CoreMatchers.is(mergedVector));
  }

  @Test
  public void shouldMergeNull() {
    // Given:
    final ConsistencyOffsetVector offsetVector = ConsistencyOffsetVector.emptyVector()
        .withComponent(TEST_TOPIC1, 1, 2L);

    // When:
    offsetVector.merge(null);

    // Then:
    assertThat(offsetVector.getTopicOffsets(TEST_TOPIC1), equalTo(ImmutableMap.of(1, 2L)));
  }
}
