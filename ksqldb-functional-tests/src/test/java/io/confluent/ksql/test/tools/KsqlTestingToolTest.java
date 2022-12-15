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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KsqlTestingToolTest {

  private static final String UTF_8 = "UTF-8";

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  private final static String CORRECT_TESTS_FOLDER = "src/test/resources/test-runner/correct/";
  private final static String INCORRECT_TESTS_FOLDER = "src/test/resources/test-runner/incorrect";

  private final static String UDF_TESTS_FOLDER = "src/test/resources/test-runner/udf/";
  private final static String UDF_EXTENSION_DIR
      = "../ksqldb-engine/src/test/resources/udf-example-2.jar";

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

    for (final File correctTestFolder : testSubFolders) {
      outContent.reset();
      errContent.reset();
      runTestCaseAndAssertPassed(correctTestFolder.getPath() + "/statements.sql",
          correctTestFolder.getPath() + "/input.json",
          correctTestFolder.getPath() + "/output.json",
          Optional.empty()
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
  public void shouldUseAndCloseTestExecutor() {
    // Given:
    final TestCase testCase = mock(TestCase.class);
    final TestExecutor testExecutor = mock(TestExecutor.class);


    // When:
    KsqlTestingTool.executeTestCase(
        testCase,
        testExecutor);

    // Then:
    verify(testExecutor).buildAndExecuteQuery(eq(testCase), any());
    verify(testExecutor).close();

  }

  @Test
  public void shouldFailWithIncorrectTest() throws Exception {
    // When:
    final int code = KsqlTestingTool.runWithTripleFiles(
        INCORRECT_TESTS_FOLDER + "/expected_mismatch/statements.sql",
        INCORRECT_TESTS_FOLDER + "/expected_mismatch/input.json",
        INCORRECT_TESTS_FOLDER + "/expected_mismatch/output.json",
        Optional.empty());

    // Then:
    assertThat(errContent.toString(UTF_8),
        containsString("Test failed: Topic 'S1', message 0: Expected <\"1001\", \"101\"> with timestamp=0 and headers=[] but was <101, \"101\"> with timestamp=0 and headers=[]\n"));
    assertEquals(1, code);
  }

  @Test
  public void shouldFailWithIncorrectInputFormat() {
    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> KsqlTestingTool.runWithTripleFiles(
            INCORRECT_TESTS_FOLDER + "/incorrect_input_format/statements.sql",
            INCORRECT_TESTS_FOLDER + "/incorrect_input_format/input.json",
            INCORRECT_TESTS_FOLDER + "/incorrect_input_format/output.json",
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "File name: " + INCORRECT_TESTS_FOLDER + "/incorrect_input_format/input.json Message: Unexpected character ('{' (code 123)): was expecting double-quote to start field name"));
  }


  @Test
  public void shouldFailWithOutputFileMissingField() {
    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> KsqlTestingTool.runWithTripleFiles(
            INCORRECT_TESTS_FOLDER + "/missing_field_in_output/statements.sql",
            INCORRECT_TESTS_FOLDER + "/missing_field_in_output/input.json",
            INCORRECT_TESTS_FOLDER + "/missing_field_in_output/output.json",
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Message: Cannot construct instance of `io.confluent.ksql.test.model.OutputRecordsNode`, problem: No 'outputs' field in the output file."));
  }

  @Test
  public void shouldFailWithEmptyInput() {
    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> KsqlTestingTool.runWithTripleFiles(
            INCORRECT_TESTS_FOLDER + "/empty_input/statements.sql",
            INCORRECT_TESTS_FOLDER + "/empty_input/input.json",
            INCORRECT_TESTS_FOLDER + "/empty_input/output.json",
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "File name: " + INCORRECT_TESTS_FOLDER + "/empty_input/input.json Message: Cannot construct instance of `io.confluent.ksql.test.model.InputRecordsNode`, problem: Inputs cannot be empty."));
  }

  @Test
  public void shouldFailWithEmptyOutput() {
    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> KsqlTestingTool.runWithTripleFiles(
            INCORRECT_TESTS_FOLDER + "/empty_output/statements.sql",
            INCORRECT_TESTS_FOLDER + "/empty_output/input.json",
            INCORRECT_TESTS_FOLDER + "/empty_output/output.json",
            Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "File name: " + INCORRECT_TESTS_FOLDER + "/empty_output/output.json Message: Cannot construct instance of `io.confluent.ksql.test.model.OutputRecordsNode`, problem: Outputs cannot be empty."));
  }

  @Test
  public void shouldPropagateInsertValuesExecutorError() throws Exception {
    // When:
    final int code = KsqlTestingTool.runWithTripleFiles(
        "src/test/resources/test-runner/incorrect/insert_values/statements.sql",
        null,
        "src/test/resources/test-runner/incorrect/insert_values/output.json",
        Optional.empty());

    // Then:
    assertThat(errContent.toString(UTF_8),
        containsString("Test failed: Failed to insert values into 'TEST'."));
    assertEquals(1, code);
  }

  @Test
  public void shouldRunProvidedExtensionDirTest() throws Exception {

    final File testFolder = new File(CORRECT_TESTS_FOLDER);
    final File[] testSubFolders = testFolder.listFiles(File::isDirectory);
    if (testSubFolders == null) {
      Assert.fail("Invalid test folder path!");
    }

    runTestCaseAndAssertPassed(UDF_TESTS_FOLDER + "/statements.sql",
        UDF_TESTS_FOLDER + "/input.json",
        UDF_TESTS_FOLDER + "/output.json",
        Optional.of(UDF_EXTENSION_DIR)
    );
  }

  private void runTestCaseAndAssertPassed(
      final String statementsFilePath,
      final String inputFilePath,
      final String outputFilePath,
      final Optional<String> extensionDir
  ) throws Exception {
    // When:
    final int code = KsqlTestingTool.runWithTripleFiles(statementsFilePath, inputFilePath, outputFilePath,
        extensionDir);

    // Then:
    final String reason = "TestFile: " + statementsFilePath
        + System.lineSeparator()
        + errContent.toString(UTF_8);

    assertThat(reason, outContent.toString(UTF_8), containsString("Test passed!"));
    assertEquals(0, code);
  }

  private void runTestCaseAndAssertPassed(
      final String statementsFilePath,
      final String outputFilePath
  ) throws Exception {
    // When:
    final int code = KsqlTestingTool.runWithTripleFiles(statementsFilePath, null, outputFilePath,
        Optional.empty());

    // Then:
    final String reason = "TestFile: " + statementsFilePath
        + System.lineSeparator()
        + errContent.toString(UTF_8);

    assertThat(reason, outContent.toString(UTF_8), containsString("Test passed!"));
    assertEquals(0, code);
  }
}
