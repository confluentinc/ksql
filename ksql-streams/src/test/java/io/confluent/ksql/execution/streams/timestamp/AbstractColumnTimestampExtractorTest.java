package io.confluent.ksql.execution.streams.timestamp;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractColumnTimestampExtractorTest {
  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfColumnIndexIsNegative() {
    // When/Then
    new AbstractColumnTimestampExtractor(-1) {
      @Override
      public long extract(final GenericRow row) {
        return 0;
      }
    };
  }

  @Test
  public void shouldCallExtractTimestampFromGenericRow() {
    // Given
    final long timestamp = 1526075913000L;

    final AbstractColumnTimestampExtractor timestampExtractor  =
        new AbstractColumnTimestampExtractor(2) {
      @Override
      public long extract(final GenericRow row) {
        return timestamp;
      }
    };

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
