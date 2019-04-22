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

package io.confluent.ksql;

import static io.confluent.ksql.EndToEndEngineTestUtil.findExpectedTopologyDirectories;
import static io.confluent.ksql.EndToEndEngineTestUtil.formatQueryName;
import static io.confluent.ksql.EndToEndEngineTestUtil.loadExpectedTopologies;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.EndToEndEngineTestUtil.TestFile;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.test.commons.TestCase;
import io.confluent.ksql.test.commons.TestCaseNode;
import io.confluent.ksql.test.commons.TopologyAndConfigs;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Runs the json functional tests defined under
 *  `ksql-engine/src/test/resources/query-validation-tests`.
 *
 *  See `ksql-engine/src/test/resources/query-validation-tests/README.md` for more info.
 */
@RunWith(Parameterized.class)
public class QueryTranslationTest {

  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private static final String TOPOLOGY_CHECKS_DIR = "expected_topology/";
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
    return Stream.of(getTopologyVersions())
        .map(version ->
            new TopologiesAndVersion(version, loadExpectedTopologies(TOPOLOGY_CHECKS_DIR + version)))
        .collect(Collectors.toList());
  }

  private static String[] getTopologyVersions() {
    String[] topologyVersions;
    final String topologyVersionsProp = System.getProperty(TOPOLOGY_VERSIONS_PROP);
    if (topologyVersionsProp != null) {
      topologyVersions = topologyVersionsProp.split(TOPOLOGY_VERSIONS_DELIMITER);
    } else {
      final List<String> topologyVersionsList = findExpectedTopologyDirectories(TOPOLOGY_CHECKS_DIR);
      topologyVersions = new String[topologyVersionsList.size()];
      topologyVersions = topologyVersionsList.toArray(topologyVersions);
    }
    return topologyVersions;
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
        final TestCase versionedTestCase = testCase.copyWithName(
            testCase.getName() + "-" + topologies.getVersion());
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

    private final String version;
    private final Map<String, TopologyAndConfigs> topologies;

    TopologiesAndVersion(final String version, final Map<String, TopologyAndConfigs> topologies) {
      this.version = version;
      this.topologies = topologies;
    }

    String getVersion() {
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
