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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.test.model.QttTestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.tools.command.TestOptions;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class KsqlTestingTool {

  private KsqlTestingTool() {

  }

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;

  private static int totalNumberOfTests;

  public static void main(final String[] args) throws IOException {
    loadAndRunTests(args);
  }

  static void loadAndRunTests(final String[] args) {
    totalNumberOfTests = 0;
    final List<String> passedTests = new ArrayList<>();
    final List<Pair<String, String>> failedTests = new ArrayList<>();
    try {

      final TestOptions testOptions = TestOptions.parse(args);

      if (testOptions == null) {
        return;
      }

      final QttTestFile qttTestFile = OBJECT_MAPPER.readValue(
          new File(testOptions.getTestFile()), QttTestFile.class);
      for (final TestCaseNode testCaseNode: qttTestFile.tests) {
        final List<TestCase> testCases = testCaseNode.buildTests(
            new File(testOptions.getTestFile()).toPath(),
            TestFunctionRegistry.INSTANCE.get());
        for (final TestCase testCase: testCases) {
          executeTestCase(
              testCase,
              new TestExecutor(),
              passedTests,
              failedTests);
        }
      }
      printResults(totalNumberOfTests, passedTests, failedTests);
    } catch (final Exception e) {
      System.err.println("Failed to start KSQL testing tool: " + e.getMessage());
    }
  }

  static void executeTestCase(
      final TestCase testCase,
      final TestExecutor testExecutor,
      final List<String> passedTests,
      final List<Pair<String, String>> failedTests
  ) {
    try {
      System.out.println(" >>> Running test: " + testCase.getName());
      testExecutor.buildAndExecuteQuery(testCase);
      System.out.println("\t >>> Test " + testCase.getName() + " passed!");
      passedTests.add(testCase.getName());
    } catch (final Exception e) {
      e.printStackTrace();
      System.err.println("\t>>>>> Test " + testCase.getName() + " failed: " + e.getMessage());
      failedTests.add(new Pair<>(testCase.getName(), e.getMessage()));
    } finally {
      testExecutor.close();
      totalNumberOfTests ++;
    }
  }

  private static void printResults(
      final int totalNumberOfTests,
      final List<String> passedTests,
      final List<Pair<String, String>> failedTests) {
    System.out.println("Number of tests: " + totalNumberOfTests);
    if (passedTests.size() == totalNumberOfTests) {
      System.out.println("All tests passed!");
      return;
    }
    System.out.println(passedTests.size() + " out of " + totalNumberOfTests + " tests passed!");
    if (!failedTests.isEmpty()) {
      System.out.println(failedTests.size() + " tests failed: ");
      System.out.println("Failing tests: ");
      for (final Pair pair: failedTests) {
        System.out.println("\t\t Test name: " + pair.getLeft()
            + " , Failure reason: " + pair.getRight());
      }
    }
  }
}
