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

import io.confluent.ksql.test.loader.TestLoader;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import io.confluent.ksql.test.tools.VersionedTest;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Loads test cases that include physical plan for any QTT test case that should be tested
 * against a saved physical plan (according to {@link PlannedTestUtils#isPlannedTestCase})
 */
public class PlannedTestLoader implements TestLoader<VersionedTest> {

  private final TestLoader<TestCase> innerLoader;

  private PlannedTestLoader(
      final TestLoader<TestCase> innerLoader
  ) {
    this.innerLoader = Objects.requireNonNull(innerLoader, "innerLoader");
  }

  public static PlannedTestLoader of(final TestLoader<TestCase> innerLoader) {
    return new PlannedTestLoader(innerLoader);
  }

  @Override
  public Stream<VersionedTest> load() {
    return innerLoader.load().flatMap(this::buildHistoricalTestCases);
  }

  private Stream<VersionedTest> buildHistoricalTestCases(final TestCase testCase) {
    if (PlannedTestUtils.isPlannedTestCase(testCase)) {
      return TestCasePlanLoader.allForTestCase(testCase).stream()
          .map(plan -> PlannedTestUtils.buildPlannedTestCase(testCase, plan));
    } else if (testCase.getVersionBounds().contains(KsqlVersion.current())) {
      return Stream.of(testCase);
    } else {
      return Stream.empty();
    }
  }
}
