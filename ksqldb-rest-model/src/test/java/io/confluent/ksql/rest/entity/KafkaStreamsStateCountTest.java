/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

@SuppressWarnings("SameParameterValue")
public class KafkaStreamsStateCountTest {

  KafkaStreamsStateCount kafkaStreamsStateCount;
  
  @Before
  public void setup() {
    kafkaStreamsStateCount = new KafkaStreamsStateCount();
  }

  @Test
  public void shouldUpdateExistingStateCount() {
    kafkaStreamsStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        kafkaStreamsStateCount.getState().get(KafkaStreams.State.RUNNING),
        is(2));
    kafkaStreamsStateCount.updateStateCount(KafkaStreams.State.RUNNING, 4);
    assertThat(
        kafkaStreamsStateCount.getState().get(KafkaStreams.State.RUNNING),
        is(6));
  }

  @Test
  public void shouldToString() {
    kafkaStreamsStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        kafkaStreamsStateCount.toString(),
        is("RUNNING:2"));
    kafkaStreamsStateCount.updateStateCount(KafkaStreams.State.NOT_RUNNING, 1);
    assertThat(
        kafkaStreamsStateCount.toString(),
        is("RUNNING:2, NOT_RUNNING:1"));
  }

  @Test
  public void shouldConvertStringToKafkaStreamsStateCount() {
    final String testString = "REBALANCING:4, ERROR:1,    RUNNING:2";
    kafkaStreamsStateCount = new KafkaStreamsStateCount(testString);
    final Map<KafkaStreams.State, Integer> state = kafkaStreamsStateCount.getState();
    assertThat(state.get(KafkaStreams.State.REBALANCING), is(4));
    assertThat(state.get(KafkaStreams.State.ERROR), is(1));
    assertThat(state.get(KafkaStreams.State.RUNNING), is(2));
    assertThat(kafkaStreamsStateCount.toString(), is("REBALANCING:4, RUNNING:2, ERROR:1"));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfInvalidFormat() {
    kafkaStreamsStateCount = new KafkaStreamsStateCount("RUNNING:2:2");
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfInvalidCount() {
    kafkaStreamsStateCount = new KafkaStreamsStateCount("RUNNING:q");
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfInvalidState() {
    kafkaStreamsStateCount = new KafkaStreamsStateCount("other state:2");
  }
}