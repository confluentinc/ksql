package io.confluent.ksql.util.timestamp;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class StringTimestampExtractionPolicyTest {
  @Test
  public void shouldTestEqualityCorrectly() {
    new EqualsTester()
        .addEqualityGroup(
            new StringTimestampExtractionPolicy("field1", "yyMMddHHmmssZ"),
            new StringTimestampExtractionPolicy("field1", "yyMMddHHmmssZ"))
        .addEqualityGroup(new StringTimestampExtractionPolicy("field2", "yyMMddHHmmssZ"))
        .addEqualityGroup(new StringTimestampExtractionPolicy("field1", "ddMMyyHHmmssZ"))
        .testEquals();
  }
}
