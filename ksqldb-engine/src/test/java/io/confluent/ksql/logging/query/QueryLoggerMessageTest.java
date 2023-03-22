/*
 * Copyright 2021 Confluent Inc.
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.confluent.io/confluent-community-license
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
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