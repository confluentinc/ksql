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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;

public class RawQueryStatusCountTest {

  private RawQueryStatusCount rawQueryStatusCount;

  @Before
  public void setup() {
    rawQueryStatusCount = new RawQueryStatusCount();
  }

  @Test
  public void shouldUpdateExistingStatusCount() {
    rawQueryStatusCount.updateRawStatusCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        rawQueryStatusCount.getStatuses().get(KafkaStreams.State.RUNNING),
        is(2));

    rawQueryStatusCount.updateRawStatusCount(KafkaStreams.State.RUNNING, 4);
    assertThat(
        rawQueryStatusCount.getStatuses().get(KafkaStreams.State.RUNNING),
        is(6));

    rawQueryStatusCount.updateRawStatusCount(KafkaStreams.State.CREATED, 2);
    assertThat(
        rawQueryStatusCount.getStatuses().get(KafkaStreams.State.CREATED),
        is(2));

    rawQueryStatusCount.updateRawStatusCount(KafkaStreams.State.ERROR, 1);
    assertThat(
        rawQueryStatusCount.getStatuses().get(KafkaStreams.State.ERROR),
        is(1));

    rawQueryStatusCount.updateRawStatusCount(KafkaStreams.State.ERROR, 3);
    assertThat(
        rawQueryStatusCount.getStatuses().get(KafkaStreams.State.ERROR),
        is(4));
  }
}
