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

package io.confluent.ksql.test.planned;

import io.confluent.ksql.test.tools.TestCase;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tool for generating new TestCasePlans and writing them to the local filesystem
 */
public final class PlannedTestGenerator {

  private PlannedTestGenerator() {
  }

  public static void generatePlans(final Stream<TestCase> testCases,
      final boolean validateResults) {
    testCases
        .filter(PlannedTestUtils::isPlannedTestCase)
        .forEach((t) -> maybeGenerateTestCase(t, validateResults));
  }

  private static void maybeGenerateTestCase(
      final TestCase testCase,
      final boolean validateTestsForCurrentSetup) {
    final Optional<TestCasePlan> latest = TestCasePlanLoader.latestForTestCase(testCase);
    final TestCasePlan current = TestCasePlanLoader.currentForTestCase(testCase,
        validateTestsForCurrentSetup);
    final boolean isSamePlan = latest.isPresent() && PlannedTestUtils.isSamePlan(latest, current);
    final boolean isSameTopology = latest.isPresent() && latest.get().getTopology().strip()
        .equals(current.getTopology().strip());
    if (isSamePlan && isSameTopology) {
      return;
    }
    TestCasePlanWriter.writeTestCasePlan(current);
  }
}
