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
import io.confluent.ksql.test.model.QttTestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.tools.command.TestOptions;
import java.io.File;
import java.io.IOException;
import java.util.List;

public final class KsqlTestingTool {

  private KsqlTestingTool() {

  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(final String[] args) throws IOException {
    loadAndRunTests(args);
  }

  static void loadAndRunTests(final String[] args) {
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
          executeTestCase(testCase, new TestExecutor());
        }
      }

      System.out.println("All tests passed!");

    } catch (final Exception e) {
      System.err.println("Failed to start KSQL testing tool: " + e.getMessage());
    }
  }

  static void executeTestCase(final TestCase testCase, final TestExecutor testExecutor) {
    try {
      System.out.println(" >>> Running test: " + testCase.getName());
      testExecutor.buildAndExecuteQuery(testCase);
      System.out.println("\t >>> Test " + testCase.getName() + " passed!");
    } catch (final Exception e) {
      e.printStackTrace();
      System.err.println("\t>>>>> Test " + testCase.getName() + " failed: " + e.getMessage());
    } finally {
      testExecutor.close();
    }
  }
}
