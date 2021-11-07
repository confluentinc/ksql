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
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class ConsistencyOffsetVectorTest {

  @Test
  public void shouldSerializeAndDeserialize() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();

    offsetVector.setVersion(1);
    offsetVector.addTopicOffsets("testTopic", ImmutableMap.of(1, 1L, 2, 2L, 3, 3L));

    // When:
    final ConsistencyOffsetVector deserializedCV =
        ConsistencyOffsetVector.deserialize(offsetVector.serialize());

    // Then:
    assertThat(deserializedCV.equals(offsetVector), is(true));
  }

  @Test
  public void shouldDominateIfGreater() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    final ConsistencyOffsetVector other = new ConsistencyOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 1, 1);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), CoreMatchers.is(true));
    assertThat(other.dominates(offsetVector), CoreMatchers.is(false));
  }

  @Test
  public void shouldDominateIfEqual() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    final ConsistencyOffsetVector other = new ConsistencyOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 1, 2);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), CoreMatchers.is(true));
    assertThat(other.dominates(offsetVector), CoreMatchers.is(true));
  }

  @Test
  public void shouldNotDominateIfLess() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 1);
    offsetVector.update("testTopic", 2, 1);

    final ConsistencyOffsetVector other = new ConsistencyOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 1, 2);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), CoreMatchers.is(false));
    assertThat(other.dominates(offsetVector), CoreMatchers.is(true));
  }

  @Test
  public void shouldDominateWithPartitionMissing() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    final ConsistencyOffsetVector other = new ConsistencyOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), CoreMatchers.is(true));
    assertThat(other.dominates(offsetVector), CoreMatchers.is(true));
  }

  @Test
  public void shouldDominateWithTopicMissing() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();
    offsetVector.update("testTopic1", 0, 1);
    offsetVector.update("testTopic1", 1, 2);
    offsetVector.update("testTopic1", 2, 1);
    offsetVector.update("testTopic2", 0, 2);
    offsetVector.update("testTopic2", 1, 1);

    final ConsistencyOffsetVector other = new ConsistencyOffsetVector();
    other.update("testTopic1", 0, 1);
    other.update("testTopic1", 2, 1);

    assertThat(offsetVector.dominates(other), CoreMatchers.is(true));
    assertThat(other.dominates(offsetVector), CoreMatchers.is(true));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final ConsistencyOffsetVector offsetVector = new ConsistencyOffsetVector();
    offsetVector.update("testTopic1", 0, 1);
    offsetVector.update("testTopic1", 1, 2);
    offsetVector.update("testTopic1", 2, 1);

    final ConsistencyOffsetVector other = new ConsistencyOffsetVector();
    other.update("testTopic1", 2, 2);
    other.update("testTopic2", 0, 1);

    offsetVector.merge(other);

    final ConsistencyOffsetVector mergedVector = new ConsistencyOffsetVector();
    mergedVector.update("testTopic1", 0, 1);
    mergedVector.update("testTopic1", 1, 2);
    mergedVector.update("testTopic1", 2, 2);

    assertThat(offsetVector, CoreMatchers.is(mergedVector));
  }

}
