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
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Tool for rewriting planned test cases
 */
public class PlannedTestRewriter {
  private final BiFunction<TestCase, TestCasePlan, TestCasePlan> rewriter;

  public static final BiFunction<TestCase, TestCasePlan, TestCasePlan> FULL
      = TestCasePlanLoader::rebuiltForTestCase;

  public PlannedTestRewriter(final BiFunction<TestCase, TestCasePlan, TestCasePlan> rewriter) {
    this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
  }

  public void rewriteTestCases(final Stream<TestCase> testCases) {
    testCases
        .filter(PlannedTestUtils::isPlannedTestCase)
        .forEach(this::rewriteTestCase);
  }

  private void rewriteTestCase(final TestCase testCase) {
    for (final TestCasePlan testCasePlan : TestCasePlanLoader.allForTestCase(testCase)) {
      final TestCasePlan rewritten = rewriter.apply(testCase, testCasePlan);
      TestCasePlanWriter.writeTestCasePlan(testCase, rewritten);
    }
  }
}
