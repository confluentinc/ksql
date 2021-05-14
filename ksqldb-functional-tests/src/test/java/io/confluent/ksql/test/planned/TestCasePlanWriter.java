/*
 * Copyright 2021 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.model.KsqlVersion;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class TestCasePlanWriter {

  private TestCasePlanWriter() {
  }

  public static void writeTestCasePlan(final TestCasePlan thePlan) {
    final Path parent = PlannedTestPath.forTestCasePlan(TestCasePlanLoader.PLANS_DIR, thePlan)
        .relativePath();

    final Path specPath = parent.resolve(PlannedTestPath.SPEC_FILE);
    final Path planPath = parent.resolve(PlannedTestPath.PLAN_FILE);
    final Path topologyPath = parent.resolve(PlannedTestPath.TOPOLOGY_FILE);

    final ObjectMapper planMapper = getPlanMapper(thePlan);

    try {
      Files.createDirectories(parent);
      Files.write(
          specPath,
          JsonTestLoader.OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
              .writeValueAsString(thePlan.getSpecNode())
              .getBytes(Charsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      );
      Files.write(
          planPath,
          planMapper.writerWithDefaultPrettyPrinter()
              .writeValueAsString(thePlan.getPlanNode())
              .getBytes(Charsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      );
      Files.write(
          topologyPath,
          thePlan.getTopology().getBytes(Charsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING
      );
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Older versions of the plan we persisted without {@code Include.NON_EMPTY} set. To give us clean
   * re-writes, we replicate that here.
   */
  private static ObjectMapper getPlanMapper(final TestCasePlan plan) {
    final KsqlVersion version = KsqlVersion.parse(plan.getSpecNode().getVersion());
    if (version.getVersion().major() <= 5) {
      return PlannedTestUtils.PLAN_MAPPER.copy()
          .setSerializationInclusion(Include.ALWAYS);
    }

    return PlannedTestUtils.PLAN_MAPPER;
  }
}
