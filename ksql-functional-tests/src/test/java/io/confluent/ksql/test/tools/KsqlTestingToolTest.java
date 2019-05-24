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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
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
  public void shouldRunCorrectsTest() throws IOException {
    final String testFolderPath = "src/test/resources/test-runner/";
    for (int i = 1; i <= 3; i++) {
      outContent.reset();
      errContent.reset();
      runTestCaseAndAssertPassed(testFolderPath + "test" + i + "/statements.sql",
          testFolderPath + "test" + i + "/input.json",
          testFolderPath + "test" + i + "/output.json"
          );
    }
  }

  @Test
  public void shouldUseAndCloseTestExecutor() throws Exception {
    // Given:
    final TestCase testCase = mock(TestCase.class);
    final TestExecutor testExecutor = mock(TestExecutor.class);


    // When:
    KsqlTestingTool.executeTestCase(
        testCase,
        testExecutor);

    // Then:
    verify(testExecutor).buildAndExecuteQuery(testCase);
    verify(testExecutor).close();

  }

  @Test
  public void shouldFailWithIncorrectTest() throws IOException {
    // When:
    KsqlTestingTool.runWithTripleFiles(
        "src/test/resources/test-runner/incorrect-test/statements.sql",
        "src/test/resources/test-runner/incorrect-test/input.json",
        "src/test/resources/test-runner/incorrect-test/output.json");

    // Then:
    assertThat(errContent.toString("UTF-8"),
        containsString("Test failed: Expected <1001, 101> with timestamp=0 but was <101, 101> with timestamp=0\n"));
  }

  @Test
  public void shouldFailWithIncorrectArgs() throws UnsupportedEncodingException {


  }


  private void runTestCaseAndAssertPassed(
      final String statementsFilePath,
      final String inputFilePath,
      final String outputFilePath
      ) throws IOException {
    // When:
    KsqlTestingTool.runWithTripleFiles(statementsFilePath, inputFilePath, outputFilePath);

    // Then:
    assertThat(outContent.toString("UTF-8"), containsString("Test passed!"));
  }
}