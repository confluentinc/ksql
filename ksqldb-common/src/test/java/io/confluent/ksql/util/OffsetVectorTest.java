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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

public class OffsetVectorTest {

  @Test
  public void shouldSerializedAndDeserialize() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    String token = offsetVector.serialize();
    OffsetVector other = OffsetVectorFactory.deserialize(token);

    assertThat(offsetVector, is(other));
  }

  @Test
  public void shouldDominateIfGreater() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    OffsetVector other = OffsetVectorFactory.createOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 1, 1);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), is(true));
    assertThat(other.dominates(offsetVector), is(false));
  }

  @Test
  public void shouldDominateIfEqual() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    OffsetVector other = OffsetVectorFactory.createOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 1, 2);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), is(true));
    assertThat(other.dominates(offsetVector), is(true));
  }

  @Test
  public void shouldNotDominateIfLess() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 1);
    offsetVector.update("testTopic", 2, 1);

    OffsetVector other = OffsetVectorFactory.createOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 1, 2);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), is(false));
    assertThat(other.dominates(offsetVector), is(true));
  }

  @Test
  public void shouldDominateWithPartitionMissing() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic", 0, 1);
    offsetVector.update("testTopic", 1, 2);
    offsetVector.update("testTopic", 2, 1);

    OffsetVector other = OffsetVectorFactory.createOffsetVector();
    other.update("testTopic", 0, 1);
    other.update("testTopic", 2, 1);

    assertThat(offsetVector.dominates(other), is(true));
    assertThat(other.dominates(offsetVector), is(true));
  }

  @Test
  public void shouldDominateWithTopicMissing() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic1", 0, 1);
    offsetVector.update("testTopic1", 1, 2);
    offsetVector.update("testTopic1", 2, 1);
    offsetVector.update("testTopic2", 0, 2);
    offsetVector.update("testTopic2", 1, 1);

    OffsetVector other = OffsetVectorFactory.createOffsetVector();
    other.update("testTopic1", 0, 1);
    other.update("testTopic1", 2, 1);

    assertThat(offsetVector.dominates(other), is(true));
    assertThat(other.dominates(offsetVector), is(true));
  }

  @Test
  public void shouldMerge() {
    // Given:
    OffsetVector offsetVector = OffsetVectorFactory.createOffsetVector();
    offsetVector.update("testTopic1", 0, 1);
    offsetVector.update("testTopic1", 1, 2);
    offsetVector.update("testTopic1", 2, 1);

    OffsetVector other = OffsetVectorFactory.createOffsetVector();
    other.update("testTopic1", 2, 2);
    other.update("testTopic2", 0, 1);

    offsetVector.merge(other);

    OffsetVector mergedVector = OffsetVectorFactory.createOffsetVector();
    mergedVector.update("testTopic1", 0, 1);
    mergedVector.update("testTopic1", 1, 2);
    mergedVector.update("testTopic1", 2, 2);

    assertThat(offsetVector, is(mergedVector));
  }
}
