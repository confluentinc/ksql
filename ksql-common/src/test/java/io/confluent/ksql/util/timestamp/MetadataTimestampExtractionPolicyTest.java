package io.confluent.ksql.util.timestamp;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class MetadataTimestampExtractionPolicyTest {
  @Test
  public void shouldTestEqualityCorrectly() {
    new EqualsTester()
        .addEqualityGroup(
            new MetadataTimestampExtractionPolicy(),
            new MetadataTimestampExtractionPolicy())
        .testEquals();
  }
}
