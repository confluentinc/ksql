package io.confluent.ksql.util.timestamp;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class LongColumnTimestampExtractionPolicyTest {
  @Test
  public void shouldTestEqualityCorrectly() {
    new EqualsTester()
        .addEqualityGroup(
            new LongColumnTimestampExtractionPolicy("field1"),
            new LongColumnTimestampExtractionPolicy("field1"))
        .addEqualityGroup(new LongColumnTimestampExtractionPolicy("field2"))
        .testEquals();
  }
}
