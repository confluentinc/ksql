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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public final class PlannedTestPath {
  private static final String INVALID_FILENAME_CHARS_PATTERN = "\\s|/|\\\\|:|\\*|\\?|\"|<|>|\\|";
  private static final String BASE_DIRECTORY = "src/test/resources/";
  private static final String PLANS_DIR = "historical_plans/";
  public static final String SPEC_FILE = "spec.json";
  public static final String TOPOLOGY_FILE = "topology";

  private final Path path;

  private PlannedTestPath(final Path path) {
    this.path = Objects.requireNonNull(path, "path");
  }

  public static PlannedTestPath forTestCase(final TestCase testCase) {
    return new PlannedTestPath(Paths.get(PLANS_DIR, formatName(testCase.getName())));
  }

  public static PlannedTestPath forTestCasePlan(final TestCase testCase, final TestCasePlan plan) {
    return new PlannedTestPath(
        forTestCase(testCase).path().resolve(
            String.format("%s_%s", plan.getVersion(), plan.getTimestamp()))
    );
  }

  public PlannedTestPath resolve(final Path path) {
    return new PlannedTestPath(this.path.resolve(path));
  }

  public PlannedTestPath resolve(final String path) {
    return new PlannedTestPath(this.path.resolve(path));
  }

  public Path path() {
    return path;
  }

  public Path relativePath() {
    return findBaseDir().resolve(path);
  }

  private static Path findBaseDir() {
    Path path = Paths.get("./ksql-functional-tests");
    if (Files.exists(path)) {
      return path.resolve(BASE_DIRECTORY);
    }
    path = Paths.get("../ksql-functional-tests");
    if (Files.exists(path)) {
      return path.resolve(BASE_DIRECTORY);
    }
    throw new RuntimeException("Failed to determine location of expected topologies directory. "
        + "App should be run with current directory set to either the root of the repo or the "
        + "root of the ksql-functional-tests module");
  }

  private static String formatName(final String originalName) {
    return originalName
        .replaceAll(" - (AVRO|JSON|DELIMITED|KAFKA)$", "")
        .replaceAll(INVALID_FILENAME_CHARS_PATTERN, "_");
  }
}

