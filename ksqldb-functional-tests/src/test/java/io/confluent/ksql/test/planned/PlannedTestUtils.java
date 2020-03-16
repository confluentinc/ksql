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
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class PlannedTestUtils {
  static final ObjectMapper PLAN_MAPPER = PlanJsonMapper.create();

  private PlannedTestUtils() {
  }

  public static boolean isPlannedTestCase(final TestCase testCase) {
    return !testCase.expectedException().isPresent()
        && !testCase.getTestFile().endsWith("/scratch.json");
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
    final Map<String, Topic> topicsByName = testCase.getTopics().stream()
        .collect(Collectors.toMap(Topic::getName, t -> t));
    final KsqlVersion version = KsqlVersion.parse(planAtVersionNode.getVersion())
        .withTimestamp(planAtVersionNode.getTimestamp());
    return testCase.withPlan(
        version,
        new TopologyAndConfigs(
            Optional.of(planAtVersionNode.getPlan()),
            planAtVersionNode.getTopology(),
            planAtVersionNode.getSchemas(),
            planAtVersionNode.getConfigs()
        ),
        planAtVersionNode.getInputs().stream()
            .map(n -> n.build(topicsByName))
            .collect(Collectors.toList()),
        planAtVersionNode.getOutputs().stream()
            .map(n -> n.build(topicsByName))
            .collect(Collectors.toList())
    );
  }
}
