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
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import io.confluent.ksql.test.tools.VersionedTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class PlannedTestUtils {
  // this is temporary
  private static final List<String> WHITELIST = ImmutableList.of(
      "average - calculate average in select"
  );

  private PlannedTestUtils() {
  }

  public static boolean isPlannedTestCase(final TestCase testCase) {
    return !testCase.expectedException().isPresent() && WHITELIST.contains(testCase.getName());
  }

  public static boolean isSamePlan(
      final Optional<TestCasePlan> latest,
      final TestCasePlan current) {
    return latest.isPresent() && current.getPlan().equals(latest.get().getPlan());
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

  public static TestCase buildPlannedTestCase(
      final TestCase testCase,
      final TestCasePlan planAtVersionNode
  ) {
    final KsqlVersion version = KsqlVersion.parse(planAtVersionNode.getVersion())
        .withTimestamp(planAtVersionNode.getTimestamp());
    return testCase.withExpectedTopology(
        version,
        new TopologyAndConfigs(
            Optional.of(planAtVersionNode.getPlan()),
            planAtVersionNode.getTopology(),
            planAtVersionNode.getSchemas(),
            planAtVersionNode.getConfigs()
        )
    );
  }
}
