/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.security.Permission;
import java.util.ArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KsqlTestingToolTest {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @Before
  public void setUpStreams() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, true, "UTF-8"));
    System.setErr(new PrintStream(errContent, true, "UTF-8"));
  }

  @After
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void shouldRunCorrectTest() throws UnsupportedEncodingException {
    // When:
    KsqlTestingTool.loadAndRunTests(new String[]{"src/test/resources/unit_test.json"});

    // Then:
    assertThat(outContent.toString("UTF-8"), containsString("All tests passed!"));
  }

  @Test
  public void shouldUseAndCloseTestExecutor() {
    // Given:
    final TestCase testCase = mock(TestCase.class);
    final TestExecutor testExecutor = mock(TestExecutor.class);


    // When:
    KsqlTestingTool.executeTestCase(
        testCase,
        testExecutor,
        new ArrayList<>(),
        new ArrayList<>());

    // Then:
    verify(testExecutor).buildAndExecuteQuery(testCase);
    verify(testExecutor).close();

  }

  @Test
  public void shouldFailWithIncorrectArgs() throws UnsupportedEncodingException {

    // Given:
    System.setSecurityManager(new TestSecurityManager());

    // When:
    try {
      KsqlTestingTool.loadAndRunTests(new String[]{"foo"});
    } catch (final Exception e) {
      assertThat(e, instanceOf(TestSecurityManager.ExitSecurityException.class));
      final TestSecurityManager.ExitSecurityException exitSecurityException = (TestSecurityManager.ExitSecurityException) e;
      assertThat(exitSecurityException.getStatus(), equalTo(-1));
      assertThat(errContent.toString("UTF-8"), equalTo("Failed to start KSQL testing tool: foo (No such file or directory)\n"));
    }
  }


  static class TestSecurityManager extends SecurityManager {

    final class ExitSecurityException extends SecurityException {
      private final int status;

      ExitSecurityException(final int status) {
        this.status = status;
      }

      int getStatus() {
        return this.status;
      }
    }

    @Override
    public void checkExit(final int status) {
      if (status != 0) {
        throw new TestSecurityManager.ExitSecurityException(status);
      }
    }

    @Override
    public void checkPermission(final Permission perm) {}

  }
}