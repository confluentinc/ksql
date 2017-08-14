/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import io.confluent.ksql.cli.LocalCli;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

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
    TestResult actual = run(command, requireOrder);
    Assert.assertEquals(expectedResult, actual);
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