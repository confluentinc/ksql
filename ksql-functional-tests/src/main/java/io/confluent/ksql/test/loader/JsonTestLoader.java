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

package io.confluent.ksql.test.loader;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.tools.Test;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Load JSON tests from a directory structure
 */
public final class JsonTestLoader<T extends Test> implements TestLoader<T> {

  // Pass a single test or multiple tests separated by commas to the test framework.
  // Example:
  //   mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json
  //   mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json,test2,json
  private static final String KSQL_TEST_FILES = "ksql.test.files";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(
      DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  static {
    OBJECT_MAPPER.registerModule(new Jdk8Module());
  }

  private final Path testDir;
  private final Class<? extends TestFile<T>> testFileType;

  public static <T extends Test> JsonTestLoader<T> of(
      final Path testDir,
      final Class<? extends TestFile<T>> testFileType
  ) {
    return new JsonTestLoader<>(
        testDir,
        testFileType
    );
  }

  private JsonTestLoader(
      final Path testDir,
      final Class<? extends TestFile<T>> testFileType
  ) {
    this.testDir = Objects.requireNonNull(testDir, "testDir");
    this.testFileType = Objects.requireNonNull(testFileType, "testFileType");
  }

  public Stream<T> load() {
    final List<String> whiteList = getWhiteList();
    final List<Path> testPaths = whiteList.isEmpty()
        ? loadTestPathsFromDirectory()
        : getTestPathsFromWhiteList(whiteList);

    final List<T> testCases = testPaths
        .stream()
        .flatMap(testPath -> buildTests(testPath, testFileType))
        .collect(Collectors.toList());

    throwOnDuplicateNames(testCases);

    return testCases.stream();
  }

  private List<Path> getTestPathsFromWhiteList(final List<String> whiteList) {
    return whiteList.stream()
        .map(name -> testDir.resolve(name.trim()))
        .collect(Collectors.toList());
  }

  private List<Path> loadTestPathsFromDirectory() {
    final InputStream s = JsonTestLoader.class.getClassLoader()
        .getResourceAsStream(testDir.toString());

    if (s == null) {
      throw new TestFrameworkException("Test directory not found: " + testDir);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(s, UTF_8))) {
      final List<Path> tests = new ArrayList<>();

      String test;
      while ((test = reader.readLine()) != null) {
        if (test.endsWith(".json")) {
          tests.add(testDir.resolve(test));
        }
      }
      return tests;
    } catch (final IOException e) {
      throw new TestFrameworkException("Failed to read test dir: " + testDir, e);
    }
  }

  private static List<String> getWhiteList() {
    final String ksqlTestFiles = System.getProperty(KSQL_TEST_FILES, "").trim();
    if (ksqlTestFiles.isEmpty()) {
      return Collections.emptyList();
    }

    return Arrays.asList(ksqlTestFiles.split(","));
  }

  private static <TFT extends TestFile<T>, T extends Test> Stream<T> buildTests(
      final Path testPath,
      final Class<TFT> testFileType
  ) {
    try (InputStream stream = JsonTestLoader.class
        .getClassLoader()
        .getResourceAsStream(testPath.toString())
    ) {
      final TFT testFile = OBJECT_MAPPER.readValue(stream, testFileType);
      return testFile.buildTests(testPath);
    } catch (final Exception e) {
      throw new RuntimeException("Unable to load test at path " + testPath, e);
    }
  }

  private static void throwOnDuplicateNames(final List<? extends Test> testCases) {
    final String duplicates = testCases.stream()
        .collect(Collectors.groupingBy(Test::getName))
        .entrySet()
        .stream()
        .filter(e -> e.getValue().size() > 1)
        .map(e -> "test name: '" + e.getKey()
            + "' found in files: " + e.getValue().stream().map(Test::getTestFile)
            .collect(Collectors.joining(",")))
        .collect(Collectors.joining(System.lineSeparator()));

    if (!duplicates.isEmpty()) {
      throw new IllegalStateException("There are tests with duplicate names: "
          + System.lineSeparator() + duplicates);
    }
  }
}
