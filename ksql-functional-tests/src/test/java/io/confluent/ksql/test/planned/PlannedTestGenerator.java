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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.test.tools.TestCase;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tool for generating new TestCasePlans and writing them to the local filesystem
 */
public class PlannedTestGenerator {
  private static final ObjectMapper MAPPER = PlanJsonMapper.create();

  public static void generatePlans(Stream<TestCase> testCases) {
    testCases
        .filter(PlannedTestUtils::isPlannedTestCase)
        .forEach(PlannedTestGenerator::maybeGenerateTestCase);
  }

  private static void maybeGenerateTestCase(final TestCase testCase) {
    final String testCaseName = PlannedTestUtils.formatName(testCase.getName());
    final Path testCaseDir = Paths.get(PlannedTestLoader.PLANS_DIR, testCaseName);
    createDirectory(testCaseDir);
    final Optional<TestCasePlan> latest = TestCasePlanLoader.fromLatest(testCaseDir);
    final TestCasePlan current = TestCasePlanLoader.fromTestCase(testCase);
    if (PlannedTestUtils.isSamePlan(latest, current)) {
      return;
    }
    dumpTestCase(testCaseDir, current);
  }

  private static String getTestDirName(final TestCasePlan planAtVersionNode) {
    return String.format("%s_%s", planAtVersionNode.getVersion(), planAtVersionNode.getTimestamp());
  }

  private static void dumpTestCase(final Path dir, final TestCasePlan planAtVersion) {
    final Path parent = PlannedTestUtils.findBaseDir()
        .resolve(dir)
        .resolve(getTestDirName(planAtVersion));
    final Path specPath = parent.resolve(PlannedTestLoader.SPEC_FILE);
    final Path topologyPath = parent.resolve(PlannedTestLoader.TOPOLOGY_FILE);
    try {
      Files.createDirectories(parent);
      Files.write(
          specPath,
          MAPPER.writerWithDefaultPrettyPrinter()
              .writeValueAsString(planAtVersion.getNode())
              .getBytes(Charsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      );
      Files.write(
          topologyPath,
          planAtVersion.getTopology().getBytes(Charsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      );
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void createDirectory(final Path path) {
    try {
      Files.createDirectories(PlannedTestUtils.findBaseDir().resolve(path));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
