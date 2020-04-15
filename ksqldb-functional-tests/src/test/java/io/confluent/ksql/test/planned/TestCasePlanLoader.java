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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestExecutionListener;
import io.confluent.ksql.test.tools.TestExecutor;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * Loads saved test case plans or builds them from a TestCase
 */
public final class TestCasePlanLoader {

  private static final StubKafkaService KAFKA_STUB = StubKafkaService.create();
  private static final String CURRENT_VERSION = getFormattedVersionFromPomFile();
  private static final KsqlConfig BASE_CONFIG = new KsqlConfig(TestExecutor.baseConfig());

  private TestCasePlanLoader() {
  }

  /**
   * Create a TestCasePlan from a TestCase by executing it against an engine
   * @param testCase the test case to build plans for
   * @return the built plan.
   */
  public static TestCasePlan currentForTestCase(final TestCase testCase) {
    return buildStatementsInTestCase(
        testCase,
        CURRENT_VERSION,
        System.currentTimeMillis()
    );
  }

  /**
   * Rebuilds a TestCasePlan given a TestCase and a TestCasePlan
   *
   * @param testCase the test case to rebuild the plan for
   * @param original the plan to rebuild
   * @return the rebuilt plan.
   */
  public static TestCasePlan rebuiltForTestCase(
      final TestCase testCase,
      final TestCasePlan original
  ) {
    return buildStatementsInTestCase(
        testCase,
        original.getSpecNode().getVersion(),
        original.getSpecNode().getTimestamp()
    );
  }

  /**
   * Create a TestCasePlan by loading it from the local filesystem. This factory loads the
   * most recent plan from a given test case directory.
   * @param testCase The test case to load the latest plan for
   * @return the loaded plan.
   */
  public static Optional<TestCasePlan> latestForTestCase(final TestCase testCase) {
    KsqlVersion latestVersion = null;
    TestCasePlan latest = null;
    for (final TestCasePlan candidate : allForTestCase(testCase)) {
      final KsqlVersion version = KsqlVersion.parse(candidate.getSpecNode().getVersion())
          .withTimestamp(candidate.getSpecNode().getTimestamp());
      if (latestVersion == null || latestVersion.compareTo(version) < 0) {
        latestVersion = version;
        latest = candidate;
      }
    }
    return Optional.ofNullable(latest);
  }

  /**
   * Create a TestCasePlan for all saved plans for a test case
   * @param testCase the test case to load saved lans for
   * @return a list of the loaded plans.
   */
  public static List<TestCasePlan> allForTestCase(final TestCase testCase) {
    final PlannedTestPath rootforCase = PlannedTestPath.forTestCase(testCase);
    return PlannedTestUtils.loadContents(rootforCase.path().toString())
        .orElse(Collections.emptyList())
        .stream()
        .map(p -> parseSpec(rootforCase.resolve(p)))
        .collect(Collectors.toList());
  }

  private static TestCasePlan parseSpec(final PlannedTestPath versionDir) {
    final PlannedTestPath planPath = versionDir.resolve(PlannedTestPath.PLAN_FILE);
    final PlannedTestPath specPath = versionDir.resolve(PlannedTestPath.SPEC_FILE);
    final PlannedTestPath topologyPath = versionDir.resolve(PlannedTestPath.TOPOLOGY_FILE);

    return new TestCasePlan(
        parseJson(specPath, JsonTestLoader.OBJECT_MAPPER, TestCaseSpecNode.class),
        parseJson(planPath, PlannedTestUtils.PLAN_MAPPER, TestCasePlanNode.class),
        slurp(topologyPath)
    );
  }

  private static <T> T parseJson(final PlannedTestPath path, final ObjectMapper mapper,
      final Class<T> type) {
    try {
      return mapper.readValue(slurp(path), type);
    } catch (final IOException e) {
      throw new TestFrameworkException("Error parsing json in file: " + path, e);
    }
  }

  private static String slurp(final PlannedTestPath path) {
    try {
      return new String(
          Files.readAllBytes(path.relativePath()),
          StandardCharsets.UTF_8
      );
    } catch (final IOException e) {
      throw new TestFrameworkException("Error reading file: " + path, e);
    }
  }

  private static TestCasePlan buildStatementsInTestCase(
      final TestCase testCase,
      final String version,
      final long timestamp
  ) {
    final TestInfoGatherer testInfo = executeTestCaseAndGatherInfo(testCase);

    final TestCaseSpecNode spec = new TestCaseSpecNode(
        version,
        timestamp,
        testInfo.getSchemasDescription(),
        testCase.getInputRecords().stream().map(RecordNode::from).collect(Collectors.toList()),
        testCase.getOutputRecords().stream().map(RecordNode::from).collect(Collectors.toList()),
        testCase.getPostConditions().asNode(testInfo.getTopics())
    );

    final TestCasePlanNode plan = new TestCasePlanNode(
        testInfo.getPlans(),
        BASE_CONFIG.getAllConfigPropsWithSecretsObfuscated()
    );

    return new TestCasePlan(
        spec,
        plan,
        testInfo.getTopologyDescription()
    );
  }

  private static TestInfoGatherer executeTestCaseAndGatherInfo(final TestCase testCase) {
    try (final TestExecutor testExecutor = TestExecutor.create()) {
      final TestInfoGatherer listener = new TestInfoGatherer();
      testExecutor.buildAndExecuteQuery(testCase, listener);
      return listener;
    } catch (final AssertionError e) {
      throw new AssertionError("Failed to run test case: " + e.getMessage()
          + System.lineSeparator()
          + "failed test: " + testCase.getName()
          + System.lineSeparator()
          + "in file: " + testCase.getTestFile(),
          e
      );
    }
  }

  private static String getFormattedVersionFromPomFile() {
    try {
      final File pomFile = new File("pom.xml");
      final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
      final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      final Document pomDoc = documentBuilder.parse(pomFile);

      final NodeList versionNodeList = pomDoc.getElementsByTagName("version");
      final String versionName = versionNodeList.item(0).getTextContent();

      return versionName.replaceAll("-SNAPSHOT?", "");
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestInfoGatherer implements TestExecutionListener {

    private final Builder<KsqlPlan> plansBuilder = new Builder<>();
    private PersistentQueryMetadata queryMetadata = null;
    private List<PostTopicNode> topics = ImmutableList.of();

    @Override
    public void acceptPlan(final ConfiguredKsqlPlan plan) {
      plansBuilder.add(plan.getPlan());
    }

    @Override
    public void acceptQuery(final PersistentQueryMetadata query) {
      queryMetadata = query;
    }

    @Override
    public void runComplete(final List<PostTopicNode> knownTopics) {
      if (queryMetadata == null) {
        throw new AssertionError("test case does not build a query");
      }

      this.topics = ImmutableList.copyOf(knownTopics);
    }

    public Map<String, String> getSchemasDescription() {
      return queryMetadata.getSchemasDescription();
    }

    public List<PostTopicNode> getTopics() {
      return topics;
    }

    public List<KsqlPlan> getPlans() {
      return plansBuilder.build();
    }

    public String getTopologyDescription() {
      return queryMetadata.getTopologyDescription();
    }
  }
}
