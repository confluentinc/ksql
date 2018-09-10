/*
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

package io.confluent.ksql;

import static junit.framework.TestCase.fail;

import io.confluent.ksql.cli.Cli;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.test.TestUtils;

public final class TestRunner {
  private final Cli localCli;
  private final TestTerminal testTerminal;

  TestRunner(final Cli localCli, final TestTerminal testTerminal) {
    this.localCli = Objects.requireNonNull(localCli, "localCli");
    this.testTerminal = Objects.requireNonNull(testTerminal, "testTerminal");
  }

  void testListOrShow(final String commandSuffix, final TestResult.OrderedResult expectedResult) {
    testListOrShow(commandSuffix, expectedResult, true);
  }

  void testListOrShow(final String commandSuffix, final TestResult expectedResult,
      final boolean requireOrder) {
    test("list " + commandSuffix, expectedResult, requireOrder);
    test("show " + commandSuffix, expectedResult, requireOrder);
  }

  void test(final String command, final TestResult.OrderedResult expectedResult) {
    test(command, expectedResult, true);
  }

  private void test(final String command, final TestResult expectedResult,
      final boolean requireOrder) {
    run(command, requireOrder);
    final Collection<List<String>> finalResults = new ArrayList<>();
    try {
      TestUtils.waitForCondition(() -> {
        final TestResult actualResult = testTerminal.getTestResult();
        finalResults.clear();
        finalResults.addAll(actualResult.data);
        return actualResult.data.containsAll(expectedResult.data);
      }, 30000, "Did not get the expected result '" + expectedResult + ", in a timely fashion.");
    } catch (final AssertionError e) {
      throw new AssertionError(
          "CLI test runner command result mismatch expected: " + expectedResult + ", actual: " + finalResults, e);
    } catch (final InterruptedException e) {
      fail("Test got interrutped when waiting for result " + expectedResult.toString());
    }
  }

  TestResult run(String command, final boolean requireOrder) {
    try {
      if (!command.endsWith(";")) {
        command += ";";
      }
      System.out.println("[Run Command] " + command);
      testTerminal.resetTestResult(requireOrder);
      localCli.handleLine(command);
      return testTerminal.getTestResult();
    } catch (final Exception e) {
      throw new AssertionError("Failed to run command: " + command, e);
    }
  }

  public TestResult run(final String command) {
    return run(command, false);
  }
}