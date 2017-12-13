/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.junit.Test;

import java.net.ConnectException;
import java.sql.SQLDataException;

import static org.junit.Assert.assertEquals;

public class CommonUtilsTest {
  @Test
  public void testGetErrorMessage() {
    Exception exception = new SQLDataException("http://localhost:1621", "", 422);

    assertEquals("http://localhost:1621", CommonUtils.getErrorMessage(exception));

    exception = new ConnectException("asdf");

    assertEquals("Could not connect to the server.", CommonUtils.getErrorMessage(exception));
  }

  class TestException extends Exception {
    TestException (String msg) { super(msg); }
    TestException (String msg, Throwable cause) { super(msg, cause); }
  }

  @Test
  public void testGetErrorCauseMessage() {
    // test the case where there is a single cause
    Exception cause = new TestException("Cause");
    Exception exception = new TestException("Exception", cause);

    assertEquals("Caused by: Cause", CommonUtils.getErrorCauseMessage(exception));

    // test the case where there are multiple causes
    Exception l2Cause = new TestException("L2 Cause");
    cause = new TestException("Cause", l2Cause);
    exception = new TestException("Exception", cause);

    assertEquals("Caused by: Cause\r\nCaused by: L2 Cause", CommonUtils.getErrorCauseMessage(exception));
  }

  @Test
  public void testGetErrorCauseMessageNoCause() {
    Exception exception = new TestException("Exception");

    assertEquals("", CommonUtils.getErrorCauseMessage(exception));
  }

  @Test
  public void testGetErrorMessageWithCause() {
    Exception cause = new TestException("Cause");
    Exception exception = new TestException("Exception", cause);

    assertEquals("Exception\r\nCaused by: Cause", CommonUtils.getErrorMessageWithCause(exception));
  }

  @Test
  public void testGetErrorMessageWithCauseNoCause() {
    Exception exception = new TestException("Exception");

    assertEquals("Exception", CommonUtils.getErrorMessageWithCause(exception));
  }
}
