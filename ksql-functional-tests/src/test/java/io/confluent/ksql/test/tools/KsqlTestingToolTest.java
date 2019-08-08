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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlTestingToolTest {

  private static final String UTF_8 = "UTF-8";

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  private final static String CORRECT_TESTS_FOLDER = "src/test/resources/test-runner/correct/";
  private final static String INCORRECT_TESTS_FOLDER = "src/test/resources/test-runner/incorrect";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUpStreams() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, true, UTF_8));
    System.setErr(new PrintStream(errContent, true, UTF_8));
  }

  @After
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  @Test
  public void shouldRunCorrectsTest() throws Exception {

    final File testFolder = new File(CORRECT_TESTS_FOLDER);
    final File[] testSubFolders = testFolder.listFiles(File::isDirectory);
    if (testSubFolders == null) {
      Assert.fail("Invalid test folder path!");
    }

    for (final File correctTestFolder: testSubFolders) {
      outContent.reset();
      errContent.reset();
      runTestCaseAndAssertPassed(correctTestFolder.getPath() + "/statements.sql",
          correctTestFolder.getPath() + "/input.json",
          correctTestFolder.getPath() + "/output.json"
          );
    }
  }

  @Test
  public void shouldRunTestWithTwoFileInput() throws Exception {
    final String testFolderPath = "src/test/resources/test-runner/";
    for (int i = 5; i <= 6; i++) {
      outContent.reset();
      errContent.reset();
      runTestCaseAndAssertPassed(testFolderPath + "test" + i + "/statements.sql",
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
  public void shouldFailWithIncorrectTest() throws Exception {
    // When:
    KsqlTestingTool.runWithTripleFiles(
        INCORRECT_TESTS_FOLDER + "/expected_mismatch/statements.sql",
        INCORRECT_TESTS_FOLDER + "/expected_mismatch/input.json",
        INCORRECT_TESTS_FOLDER + "/expected_mismatch/output.json");

    // Then:
    assertThat(errContent.toString(UTF_8),
        containsString("Test failed: Expected <1001, 101> with timestamp=0 but was <101, 101> with timestamp=0\n"));
  }

  @Test
  public void shouldFailWithIncorrectInputFormat() throws Exception {
    // Given:
    expectedException.expect(Exception.class);
    expectedException.expectMessage("File name: " + INCORRECT_TESTS_FOLDER + "/incorrect_input_format/input.json Message: Unexpected character ('{' (code 123)): was expecting double-quote to start field name");

    // When:
    KsqlTestingTool.runWithTripleFiles(
        INCORRECT_TESTS_FOLDER + "/incorrect_input_format/statements.sql",
        INCORRECT_TESTS_FOLDER + "/incorrect_input_format/input.json",
        INCORRECT_TESTS_FOLDER + "/incorrect_input_format/output.json");

  }


  @Test
  public void shouldFailWithOutputFileMissingField() throws Exception {
    // Given:
    expectedException.expect(Exception.class);
    expectedException.expectMessage("Message: Cannot construct instance of `io.confluent.ksql.test.model.OutputRecordsNode`, problem: No 'outputs' field in the output file.");

    // When:
    KsqlTestingTool.runWithTripleFiles(
        INCORRECT_TESTS_FOLDER + "/missing_field_in_output/statements.sql",
        INCORRECT_TESTS_FOLDER + "/missing_field_in_output/input.json",
        INCORRECT_TESTS_FOLDER + "/missing_field_in_output/output.json");

  }

  @Test
  public void shouldFailWithEmptyInput() throws Exception {
    // Given:
    expectedException.expect(Exception.class);
    expectedException.expectMessage("File name: " + INCORRECT_TESTS_FOLDER + "/empty_input/input.json Message: Cannot construct instance of `io.confluent.ksql.test.model.InputRecordsNode`, problem: Inputs cannot be empty.");

    // When:
    KsqlTestingTool.runWithTripleFiles(
        INCORRECT_TESTS_FOLDER + "/empty_input/statements.sql",
        INCORRECT_TESTS_FOLDER + "/empty_input/input.json",
        INCORRECT_TESTS_FOLDER + "/empty_input/output.json");

  }

  @Test
  public void shouldFailWithEmptyOutput() throws Exception {
    // Given:
    expectedException.expect(Exception.class);
    expectedException.expectMessage("File name: " + INCORRECT_TESTS_FOLDER + "/empty_output/output.json Message: Cannot construct instance of `io.confluent.ksql.test.model.OutputRecordsNode`, problem: Outputs cannot be empty.");

    // When:
    KsqlTestingTool.runWithTripleFiles(
        INCORRECT_TESTS_FOLDER + "/empty_output/statements.sql",
        INCORRECT_TESTS_FOLDER + "/empty_output/input.json",
        INCORRECT_TESTS_FOLDER + "/empty_output/output.json");

  }

  @Test
  public void shouldPropegateInsertValuesExecutorError() throws Exception {
    // When:
    KsqlTestingTool.runWithTripleFiles(
            "src/test/resources/test-runner/incorrect-test6/statements.sql",
            null,
            "src/test/resources/test-runner/incorrect-test6/output.json");

    // Then:
    assertThat(errContent.toString(UTF_8),
            containsString("Test failed: Failed to insert values into stream/table: TEST\n"));
  }

  private void runTestCaseAndAssertPassed(
      final String statementsFilePath,
      final String inputFilePath,
      final String outputFilePath
      ) throws Exception {
    // When:
    KsqlTestingTool.runWithTripleFiles(statementsFilePath, inputFilePath, outputFilePath);

    // Then:
    final String reason = "TestFile: " + statementsFilePath
        + System.lineSeparator()
        + errContent.toString(UTF_8);

    assertThat(reason, outContent.toString(UTF_8), containsString("Test passed!"));
  }

  private void runTestCaseAndAssertPassed(
          final String statementsFilePath,
          final String outputFilePath
  ) throws Exception {
    // When:
    KsqlTestingTool.runWithTripleFiles(statementsFilePath, null, outputFilePath);

    // Then:
    final String reason = "TestFile: " + statementsFilePath
        + System.lineSeparator()
        + errContent.toString(UTF_8);

    assertThat(reason, outContent.toString(UTF_8), containsString("Test passed!"));
  }
}
