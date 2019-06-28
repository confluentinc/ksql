/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.test;

import static io.confluent.ksql.test.EndToEndEngineTestUtil.findContentsOfDirectory;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.formatQueryName;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.loadContents;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.loadExpectedTopologies;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.model.SemanticVersion;
import io.confluent.ksql.test.EndToEndEngineTestUtil.TestFile;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TopologyAndConfigs;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Runs the json functional tests defined under
 *  `ksql-functional-tests/src/test/resources/query-validation-tests`.
 *
 *  See `ksql-functional-tests/README.md` for more info.
 */
@RunWith(Parameterized.class)
public class QueryTranslationTest {

  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private static final String TOPOLOGY_CHECKS_DIR = "expected_topology/";
  static final String TOPOLOGY_VERSION_FILE = "__version";
  private static final Pattern TOPOLOGY_VERSION_PATTERN = Pattern.compile("(\\d+)_(\\d+)(_\\d+)?");
  private static final String TOPOLOGY_VERSIONS_DELIMITER = ",";
  private static final String TOPOLOGY_VERSIONS_PROP = "topology.versions";

  private final TestCase testCase;

  /**
   * @param name  - unused. Is just so the tests get named.
   * @param testCase - testCase to run.
   */
  @SuppressWarnings("unused")
  public QueryTranslationTest(final String name, final TestCase testCase) {
    this.testCase = requireNonNull(testCase, "testCase");
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(testCase);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return buildTestCases()
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private static List<TopologiesAndVersion> loadTopologiesAndVersions() {
    return getTopologyVersions().stream()
        .map(version -> new TopologiesAndVersion(
            version,
            loadExpectedTopologies(TOPOLOGY_CHECKS_DIR + version.getName())
            ))
        .collect(Collectors.toList());
  }

  private static List<KsqlVersion> getTopologyVersions() {
    final String versionProp = System.getProperty(TOPOLOGY_VERSIONS_PROP);

    final Stream<String> versionStrings = versionProp == null
        ? findExpectedTopologyDirectories().stream()
        : Arrays.stream(versionProp.split(TOPOLOGY_VERSIONS_DELIMITER));

    return versionStrings
        .map(QueryTranslationTest::getVersion)
        .collect(Collectors.toList());
  }

  private static List<String> findExpectedTopologyDirectories() {
    try {
      return findContentsOfDirectory(TOPOLOGY_CHECKS_DIR);
    } catch (final Exception e) {
      throw new RuntimeException("Could not find expected topology directories.", e);
    }
  }

  private static KsqlVersion getVersion(final String dir) {
    final Path versionFile = Paths.get(TOPOLOGY_CHECKS_DIR, dir, TOPOLOGY_VERSION_FILE);

    try {
      final String versionString = loadContents(versionFile.toString())
          .map(content -> String.join("", content))
          .orElse(dir);

      final Matcher matcher = TOPOLOGY_VERSION_PATTERN.matcher(versionString);
      if (!matcher.matches()) {
        throw new RuntimeException("Version does not match required pattern. "
            + TOPOLOGY_VERSION_PATTERN
            + ". Correct the directory name, or add a " + TOPOLOGY_VERSION_FILE + ".");
      }

      final int major = Integer.parseInt(matcher.group(1));
      final int minor = Integer.parseInt(matcher.group(2));
      final int patch = matcher.groupCount() == 3
          ? 0
          : Integer.parseInt(matcher.group(3).substring(1));

      return KsqlVersion.of(dir, SemanticVersion.of(major, minor, patch));
    } catch (Exception e) {
      throw new RuntimeException("Failed to load version file: " + versionFile, e);
    }
  }

  private static Stream<TestCase> buildVersionedTestCases(
      final TestCase testCase, final List<TopologiesAndVersion> expectedTopologies) {
    Stream.Builder<TestCase> builder = Stream.builder();
    builder = builder.add(testCase);

    for (final TopologiesAndVersion topologies : expectedTopologies) {
      final TopologyAndConfigs topologyAndConfigs =
          topologies.getTopology(formatQueryName(testCase.getName()));
      // could be null if the testCase has expected errors, no topology or configs saved
      if (topologyAndConfigs != null) {
        final TestCase versionedTestCase = testCase.withVersion(topologies.getVersion());
        versionedTestCase.setExpectedTopology(topologyAndConfigs);
        builder = builder.add(versionedTestCase);
      }
    }
    return builder.build();
  }

  private static Stream<TestCase> buildTestCases() {
    final List<TopologiesAndVersion> expectedTopologies = loadTopologiesAndVersions();

    return findTestCases()
        .flatMap(q -> buildVersionedTestCases(q, expectedTopologies));
  }

  static Stream<TestCase> findTestCases() {
    final List<String> testFiles = EndToEndEngineTestUtil.getTestFilesParam();
    return EndToEndEngineTestUtil
        .findTestCases(QUERY_VALIDATION_TEST_DIR, testFiles, QttTestFile.class);
  }

  private static class TopologiesAndVersion {

    private final KsqlVersion version;
    private final Map<String, TopologyAndConfigs> topologies;

    TopologiesAndVersion(final KsqlVersion version, final Map<String, TopologyAndConfigs> topologies) {
      this.version = Objects.requireNonNull(version, "version");
      this.topologies = Objects.requireNonNull(topologies, "topologies");
    }

    KsqlVersion getVersion() {
      return version;
    }

    TopologyAndConfigs getTopology(final String name) {
      return topologies.get(name);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class QttTestFile implements TestFile<TestCase> {

    private final List<TestCaseNode> tests;

    QttTestFile(@JsonProperty("tests") final List<TestCaseNode> tests) {
      this.tests = ImmutableList.copyOf(requireNonNull(tests, "tests collection missing"));

      if (tests.isEmpty()) {
        throw new IllegalArgumentException("test file did not contain any tests");
      }
    }

    @Override
    public Stream<TestCase> buildTests(final Path testPath) {
      return tests
          .stream()
          .flatMap(node -> node.buildTests(testPath, TestFunctionRegistry.INSTANCE.get()).stream());
    }
  }
}