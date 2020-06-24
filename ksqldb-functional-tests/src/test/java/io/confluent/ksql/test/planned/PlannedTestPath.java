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

import static com.google.common.io.Files.getNameWithoutExtension;

import io.confluent.ksql.test.tools.TestCase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public final class PlannedTestPath {

  private static final String INVALID_FILENAME_CHARS_PATTERN = "\\s|/|\\\\|:|\\*|\\?|\"|<|>|\\|";
  private static final String BASE_DIRECTORY = "src/test/resources/";
  public static final String SPEC_FILE = "spec.json";
  public static final String PLAN_FILE = "plan.json";
  public static final String TOPOLOGY_FILE = "topology";

  private final Path path;

  private PlannedTestPath(final Path path) {
    this.path = Objects.requireNonNull(path, "path");
  }

  public static PlannedTestPath of(final Path path) {
    return new PlannedTestPath(path);
  }

  public static PlannedTestPath forTestCase(final Path planDir, final TestCase testCase) {
    return new PlannedTestPath(planDir)
        .resolve(formatName(testCase.getName()));
  }

  @SuppressWarnings("UnstableApiUsage")
  public static PlannedTestPath forTestCasePlan(final Path planDir, final TestCasePlan plan) {
    final TestCaseSpecNode spec = plan.getSpecNode();

    final String fileName = getNameWithoutExtension(spec.getPath());

    final String name = formatName(fileName + " - " + spec.getTestCase().name());

    return new PlannedTestPath(planDir)
        .resolve(name)
        .resolve(spec.getVersion() + "_" + spec.getTimestamp());
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

  public Path absolutePath() {
    return relativePath().toAbsolutePath().normalize();
  }

  private static Path findBaseDir() {
    Path path = Paths.get("./ksqldb-functional-tests");
    if (Files.exists(path)) {
      return path.resolve(BASE_DIRECTORY);
    }
    path = Paths.get("../ksqldb-functional-tests");
    if (Files.exists(path)) {
      return path.resolve(BASE_DIRECTORY);
    }
    throw new RuntimeException("Failed to determine location of expected topologies directory. "
        + "App should be run with current directory set to either the root of the repo or the "
        + "root of the ksql-functional-tests module");
  }

  private static String formatName(final String originalName) {
    return originalName
        .replaceAll(INVALID_FILENAME_CHARS_PATTERN, "_");
  }

  @Override
  public String toString() {
    return path.toString();
  }
}

