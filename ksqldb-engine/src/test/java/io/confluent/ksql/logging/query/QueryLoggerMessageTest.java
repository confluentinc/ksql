package io.confluent.ksql.logging.query;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.util.QueryGuid;
import org.junit.Test;

public class QueryLoggerMessageTest {
  private static final String TEST_QUERY = "describe stream1;";
  private static final String TEST_MESSAGE = "test message";
  private static final QueryGuid GUID = new QueryGuid("test_namespace", TEST_MESSAGE, TEST_MESSAGE);


  @Test
  public void toStringShouldReturnCorrectQueryLoggerMessageString() {
    // without query guid
    QueryLoggerMessage message = new QueryLoggerMessage(TEST_MESSAGE, TEST_QUERY);
    assertEquals("test message: describe stream1;", message.toString());

    // with query guid
    message = new QueryLoggerMessage(TEST_MESSAGE, TEST_QUERY, GUID);
    assertEquals(String.format("test message (%s): describe stream1;", GUID.getQueryGuid()), message.toString());
  }
}
