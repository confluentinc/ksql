package io.confluent.ksql.execution.streams.timestamp;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

public class KsqlTimestampExtractorTest {
  @Test
  public void shouldCallExtractTimestampFromGenericRow() {
    // Given
    final long timestamp = 1526075913000L;
    final KsqlTimestampExtractor timestampExtractor  = (row -> timestamp);

    // When
    final long actualTime = timestampExtractor.extract(new ConsumerRecord<>("topic",
            1,
            1,
            null,
            genericRow(0)),
        1
    );

    // Then
    assertThat(actualTime, equalTo(timestamp));
  }
}
