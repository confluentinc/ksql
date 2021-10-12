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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestCaseBuilder;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class PlannedTestUtils {

  static final ObjectMapper PLAN_MAPPER = PlanJsonMapper.INSTANCE.get();

  private PlannedTestUtils() {
  }

  public static boolean isPlannedTestCase(final TestCase testCase) {
    return !testCase.expectedException().isPresent()
        && !testCase.getTestLocation().getTestPath().toString().endsWith("/scratch.json");
  }

  public static boolean isNotExcluded(final TestCase testCase) {
    // Place temporary logic here to exclude test cases based on feature flags, etc.
    return true;
  }

  public static boolean isSamePlan(
      final Optional<TestCasePlan> latest,
      final TestCasePlan current) {
    return latest.isPresent() && current.getPlanNode().getPlan()
        .equals(latest.get().getPlanNode().getPlan());
  }

  public static Optional<List<String>> loadContents(final String path) {
    final InputStream s = PlannedTestUtils.class.getClassLoader()
        .getResourceAsStream(path);

    if (s == null) {
      return Optional.empty();
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(s, UTF_8))) {
      final List<String> contents = new ArrayList<>();
      String file;
      while ((file = reader.readLine()) != null) {
        contents.add(file);
      }
      return Optional.of(contents);
    } catch (final IOException e) {
      throw new AssertionError("Failed to read path: " + path, e);
    }
  }

  public static TestCase buildPlannedTestCase(final TestCasePlan testCasePlan) {
    final KsqlVersion version = KsqlVersion.parse(testCasePlan.getSpecNode().getVersion())
        .withTimestamp(testCasePlan.getSpecNode().getTimestamp());

    final TestCase testCase = Iterables.getOnlyElement(TestCaseBuilder.buildTests(
        testCasePlan.getSpecNode().getTestCase(),
        Paths.get(testCasePlan.getSpecNode().getPath()),
        testName -> testCasePlan.getLocation()
    ));

    return testCase.withExpectedTopology(
        version,
        new TopologyAndConfigs(
            Optional.of(testCasePlan.getPlanNode().getPlan()),
            testCasePlan.getTopology(),
            testCasePlan.getSpecNode().getSchemas(),
            testCasePlan.getPlanNode().getConfigs()
        )
    );
  }
}
