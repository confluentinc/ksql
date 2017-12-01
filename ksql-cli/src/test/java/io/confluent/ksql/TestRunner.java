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

package io.confluent.ksql;

import io.confluent.ksql.cli.LocalCli;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static junit.framework.TestCase.fail;

public abstract class TestRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestRunner.class);

  private static LocalCli localCli;
  private static TestTerminal testTerminal;

  public static void setup(LocalCli localCli, TestTerminal testTerminal) {
    Objects.requireNonNull(localCli);
    Objects.requireNonNull(testTerminal);
    TestRunner.localCli = localCli;
    TestRunner.testTerminal = testTerminal;
  }

  protected static void testListOrShow(String commandSuffix, TestResult.OrderedResult expectedResult) {
    testListOrShow(commandSuffix, expectedResult, true);
  }

  protected static void testListOrShow(String commandSuffix, TestResult expectedResult, boolean requireOrder) {
    test("list " + commandSuffix, expectedResult, requireOrder);
    test("show " + commandSuffix, expectedResult, requireOrder);
  }

  protected static void test(String command, TestResult.OrderedResult expectedResult) {
    test(command, expectedResult, true);
  }

  protected static void test(String command, TestResult expectedResult, boolean requireOrder) {
    run(command, requireOrder);
    try {
      TestUtils.waitForCondition(() -> {
        TestResult actualResult = testTerminal.getTestResult();
        return actualResult.data.containsAll(expectedResult.data);
      }, 10000, "Did not get the expected result '" + expectedResult.toString() + ", in a timely fashion.");
    } catch (InterruptedException e) {
      fail("Test got interrutped when waiting for result " + expectedResult.toString());
    }

  }

  protected static TestResult run(String command, boolean requireOrder) throws CliTestFailedException {
    try {
      if (!command.endsWith(";")) {
        command += ";";
      }
      System.out.println("[Run Command] " + command);
      testTerminal.resetTestResult(requireOrder);
      localCli.handleLine(command);
      return testTerminal.getTestResult();
    } catch (Exception e) {
      throw new CliTestFailedException(e);
    }
  }

  protected static TestResult run(String command) throws CliTestFailedException {
    return run(command, false);
  }

}