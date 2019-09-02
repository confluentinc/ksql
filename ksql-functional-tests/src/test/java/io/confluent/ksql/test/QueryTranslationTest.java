/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.test;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.test.loader.ExpectedTopologiesTestLoader;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.loader.TestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.tools.TestCase;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Runs the json functional tests defined under
 *  `ksql-functional-tests/src/test/resources/query-validation-tests`.
 *
 *  See `ksql-functional-tests/README.md` for more info.
 */
@RunWith(Parameterized.class)
public class QueryTranslationTest {

  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private static final String TOPOLOGY_CHECKS_DIR = "expected_topology/";

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return ExpectedTopologiesTestLoader.of(testFileLoader(), TOPOLOGY_CHECKS_DIR)
        .load()
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toCollection(ArrayList::new));
  }

  static Stream<TestCase> findTestCases() {
    return testFileLoader().load();
  }

  private final TestCase testCase;

  /**
   * @param name  - unused. Is just so the tests get named.
   * @param testCase - testCase to run.
   */
  @SuppressWarnings("unused")
  public QueryTranslationTest(final String name, final TestCase testCase) {
    this.testCase = requireNonNull(testCase, "testCase");
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(testCase);
  }

  private static JsonTestLoader<TestCase> testFileLoader() {
    return JsonTestLoader.of(QUERY_VALIDATION_TEST_DIR, QttTestFile.class);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class QttTestFile implements TestFile<TestCase> {

    private final List<TestCaseNode> tests;

    QttTestFile(@JsonProperty("tests") final List<TestCaseNode> tests) {
      this.tests = ImmutableList.copyOf(requireNonNull(tests, "tests collection missing"));

      if (tests.isEmpty()) {
        throw new IllegalArgumentException("test file did not contain any tests");
      }
    }

    @Override
    public Stream<TestCase> buildTests(final Path testPath) {
      return tests
          .stream()
          .flatMap(node -> node.buildTests(testPath, TestFunctionRegistry.INSTANCE.get()).stream());
    }
  }
}