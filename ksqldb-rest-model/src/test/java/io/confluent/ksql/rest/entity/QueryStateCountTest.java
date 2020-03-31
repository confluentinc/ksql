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

import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

@SuppressWarnings("SameParameterValue")
public class QueryStateCountTest {

  QueryStateCount queryStateCount;
  
  @Before
  public void setup() {
    queryStateCount = new QueryStateCount();
  }

  @Test
  public void shouldUpdateExistingStateCount() {
    queryStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStateCount.getState().get(KafkaStreams.State.RUNNING),
        is(2));
    queryStateCount.updateStateCount(KafkaStreams.State.RUNNING, 4);
    assertThat(
        queryStateCount.getState().get(KafkaStreams.State.RUNNING),
        is(6));
  }

  @Test
  public void shouldToString() {
    queryStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStateCount.toString(),
        is("RUNNING:2"));
    queryStateCount.updateStateCount(KafkaStreams.State.NOT_RUNNING, 1);
    assertThat(
        queryStateCount.toString(),
        is("RUNNING:2,NOT_RUNNING:1"));
  }

  @Test
  public void shouldConvertStringToKafkaStreamsStateCount() {
    // Given:
    final String testString = "REBALANCING:4, ERROR:1,    RUNNING:2";
    
    // When:
    queryStateCount = new QueryStateCount(testString);
    
    // Then:
    final Map<KafkaStreams.State, Integer> state = queryStateCount.getState();
    assertThat(state.get(KafkaStreams.State.REBALANCING), is(4));
    assertThat(state.get(KafkaStreams.State.ERROR), is(1));
    assertThat(state.get(KafkaStreams.State.RUNNING), is(2));
    assertThat(queryStateCount.toString(), is("REBALANCING:4,RUNNING:2,ERROR:1"));
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfInvalidFormat() {
    queryStateCount = new QueryStateCount("RUNNING:2:2");
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfInvalidCount() {
    queryStateCount = new QueryStateCount("RUNNING:q");
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionIfInvalidState() {
    queryStateCount = new QueryStateCount("other state:2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIllegalArgumentExceptionIfDuplicateState() {
    queryStateCount = new QueryStateCount("RUNNING:4,RUNNING:2,");
  }
}