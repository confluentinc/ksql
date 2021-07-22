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
