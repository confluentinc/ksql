/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.tools.test.model.LocationWithinFile;
import io.confluent.ksql.tools.test.model.TestLocation;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Context passed around while processing a test file
 */
public class TestFileContext {

  private static final Path RESOURCE_ROOT = getRoot()
      .resolve("src")
      .resolve("test")
      .resolve("resources");

  private final Path originalFileName;
  private final Path testPath;
  private final ImmutableList<String> lines;

  public TestFileContext(final Path testPath, final List<String> lines) {
    this.originalFileName = requireNonNull(testPath, "testPath");
    this.testPath = RESOURCE_ROOT.resolve(originalFileName);
    this.lines = ImmutableList.copyOf(lines);
  }

  public Path getOriginalFileName() {
    return originalFileName;
  }

  public TestLocation getFileLocation() {
    return new PathLocation(testPath);
  }

  public TestLocation getTestLocation(final String testName) {
    return new LocationWithinFile(testPath, getLineNumber(testName));
  }

  private int getLineNumber(final String testName) {
    final Pattern pattern = Pattern
        .compile(".*\"name\"\\s*:\\s*\"" + Pattern.quote(testName) + "\".*");

    int lineNumber = 0;
    for (final String line : lines) {
      lineNumber++;

      if (pattern.matcher(line).matches()) {
        return lineNumber;
      }
    }

    throw new TestFrameworkException("Can't find test in test file"
        + System.lineSeparator()
        + "test: " + testName
        + System.lineSeparator()
        + "file: " + testPath
    );
  }

  private static Path getRoot() {
    final Path currentDir = Paths.get("").toAbsolutePath();
    if (currentDir.endsWith("ksqldb-functional-tests")) {
      return currentDir;
    }
    return currentDir.resolve("ksqldb-functional-tests");
  }
}
