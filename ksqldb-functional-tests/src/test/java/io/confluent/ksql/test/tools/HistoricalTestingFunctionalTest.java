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

package io.confluent.ksql.test.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Iterables;
import io.confluent.ksql.test.planned.PlannedTestLoader;
import io.confluent.ksql.test.planned.TestCasePlanLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Test;

/**
 * Functional test for the running of historic tests, i.e. TestCasePlan, by QTT.
 */
public class HistoricalTestingFunctionalTest {

  private static final Path TEST_BASE_DIR = Paths.get("qtt_test_cases");

  @Test
  public void shouldPassIfEverythingMatches() {
    // Given:
    final TestCase testCase = loadTestCase("correct", "simple");

    // When:
    execute(testCase);

    // Then: does not throw.
  }

  @Test
  public void shouldFailOnTopologyMismatch() {
    // Given:
    final TestCase testCase = loadTestCase("incorrect", "topology_mismatch");

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> execute(testCase)
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("Generated topology differs from that built by previous versions of KSQL")
    );
  }

  @Test
  public void shouldFailOnValueSchemaMismatch() {
    // Given:
    final TestCase testCase = loadTestCase("incorrect", "value_schema_mismatch");

    // When:
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> execute(testCase)
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString(
            "Schemas used by topology differ from those used by previous versions of KSQL")
    );
  }

  private static TestCase loadTestCase(final String path0, final String path1) {
    final Path path = TEST_BASE_DIR.resolve(path0).resolve(path1);

    final PlannedTestLoader loader = new PlannedTestLoader(
        new TestCasePlanLoader(TEST_BASE_DIR.resolve(path0)),
        testPath -> testPath.startsWith(path)
    );

    final List<TestCase> tests = loader
        .loadTests()
        .collect(Collectors.toList());

    return Iterables.getOnlyElement(tests);
  }

  private static void execute(final TestCase testCase) {
    try (final TestExecutor exec = TestExecutor.create(true, Optional.empty())) {
      exec.buildAndExecuteQuery(testCase, TestExecutionListener.noOp());
    }
  }
}
