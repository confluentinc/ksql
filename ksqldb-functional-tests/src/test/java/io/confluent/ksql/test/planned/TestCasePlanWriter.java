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
import java.nio.file.StandardOpenOption;

public class TestCasePlanWriter {
  private static final ObjectMapper MAPPER = PlanJsonMapper.create();

  public static void writeTestCasePlan(final TestCase testCase, final TestCasePlan planAtVersion) {
    final Path parent = PlannedTestPath.forTestCasePlan(testCase, planAtVersion).relativePath();
    final Path specPath = parent.resolve(PlannedTestPath.SPEC_FILE);
    final Path topologyPath = parent.resolve(PlannedTestPath.TOPOLOGY_FILE);
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
}
