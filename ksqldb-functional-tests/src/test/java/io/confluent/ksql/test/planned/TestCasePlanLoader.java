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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.model.PathLocation;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.SchemaNode;
import io.confluent.ksql.test.model.SourceNode;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.model.TopicNode;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestCaseBuilderUtil;
import io.confluent.ksql.test.tools.TestExecutionListener;
import io.confluent.ksql.test.tools.TestExecutor;
import io.confluent.ksql.tools.test.TestFunctionRegistry;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * Loads saved test case plans or builds them from a TestCase
 */
public final class TestCasePlanLoader {

  private static final KsqlVersion CURRENT_VERSION = getFormattedVersionFromPomFile();
  private static final KsqlConfig BASE_CONFIG = new KsqlConfig(TestExecutor.baseConfig());
  public static final Path PLANS_DIR = Paths.get("historical_plans");

  private final Path plansDir;

  public TestCasePlanLoader() {
    this(PLANS_DIR);
  }

  @VisibleForTesting
  public TestCasePlanLoader(final Path plansDir) {
    this.plansDir = requireNonNull(plansDir, "plansDir");
  }

  public Stream<TestCasePlan> all() {
    return load(path -> true);
  }

  public Stream<TestCasePlan> load(final Predicate<Path> predicate) {
    return PlannedTestUtils.loadContents(plansDir.toString())
        .orElseThrow(() -> new TestFrameworkException(
            "Historical test directory not found: " + plansDir))
        .stream()
        .map(plansDir::resolve)
        .filter(predicate)
        .flatMap(dir -> PlannedTestUtils.loadContents(dir.toString())
            .orElseGet(ImmutableList::of)
            .stream()
            .map(dir::resolve)
        )
        .map(PlannedTestPath::of)
        .map(TestCasePlanLoader::parseSpec);
  }

  /**
   * Create a TestCasePlan from a TestCase by executing it against an engine
   *
   * @param testCase the test case to build plans for
   * @return the built plan.
   */
  public static TestCasePlan currentForTestCase(final TestCase testCase) {
    return buildStatementsInTestCase(
        testCase,
        CURRENT_VERSION,
        System.currentTimeMillis(),
        BASE_CONFIG.getAllConfigPropsWithSecretsObfuscated(),
        TestCaseBuilderUtil.extractSimpleTestName(
            testCase.getOriginalFileName().toString(),
            testCase.getName()
        ),
        true
    );
  }

  /**
   * Rebuilds a TestCasePlan given a TestCase and a TestCasePlan
   *
   * @param original the plan to rebuild
   * @return the rebuilt plan.
   */
  public static TestCasePlan rebuild(
      final TestCasePlan original
  ) {
    return buildStatementsInTestCase(
        PlannedTestUtils.buildPlannedTestCase(original),
        KsqlVersion.parse(original.getSpecNode().getVersion()),
        original.getSpecNode().getTimestamp(),
        original.getPlanNode().getConfigs(),
        original.getSpecNode().getTestCase().name(),
        false
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
   * @param testCase the test case to load saved plans for
   * @return a list of the loaded plans.
   */
  public static List<TestCasePlan> allForTestCase(final TestCase testCase) {
    final PlannedTestPath rootforCase = PlannedTestPath.forTestCase(PLANS_DIR, testCase);
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
        new PathLocation(versionDir.absolutePath()),
        parseJson(specPath, JsonTestLoader.OBJECT_MAPPER, TestCaseSpecNode.class),
        parseJson(planPath, PlannedTestUtils.PLAN_MAPPER, TestCasePlanNode.class),
        slurp(topologyPath)
    );
  }

  private static <T> T parseJson(
      final PlannedTestPath path,
      final ObjectMapper mapper,
      final Class<T> type
  ) {
    try {
      return mapper.readValue(slurp(path), type);
    } catch (final IOException e) {
      throw new TestFrameworkException("Error parsing json in file://" + path.absolutePath(), e);
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
      final KsqlVersion version,
      final long timestamp,
      final Map<String, String> configs,
      final String simpleTestName,
      final boolean validateResults
  ) {
    final TestInfoGatherer testInfo = executeTestCaseAndGatherInfo(testCase, validateResults);

    final List<TopicNode> allTopicNodes = getTopicsFromTestCase(testCase, configs);

    final TestCaseNode testCodeNode = new TestCaseNode(
        simpleTestName,
        Optional.empty(),
        ImmutableList.of(),
        ImmutableList.of(),
        testCase.getInputRecords().stream().map(RecordNode::from).collect(Collectors.toList()),
        testCase.getOutputRecords().stream().map(RecordNode::from).collect(Collectors.toList()),
        allTopicNodes,
        testCase.statements(),
        testCase.properties(),
        null,
        testCase.getPostConditions().asNode(testInfo.getTopics(), testInfo.getSources()),
        true
    );

    final TestCaseSpecNode spec = new TestCaseSpecNode(
        version.getVersion().toString(),
        timestamp,
        testCase.getOriginalFileName().toString(),
        testInfo.getSchemas(),
        testCodeNode
    );

    final TestCasePlanNode plan = new TestCasePlanNode(testInfo.getPlans(), configs);

    return new TestCasePlan(
        new PathLocation(Paths.get("").toAbsolutePath()), // not used
        spec,
        plan,
        testInfo.getTopologyDescription()
    );
  }

  private static List<TopicNode> getTopicsFromTestCase(
      final TestCase testCase,
      final Map<?, ?> configs
  ) {
    final Collection<Topic> allTopics = TestCaseBuilderUtil.getAllTopics(
        testCase.statements(),
        testCase.getTopics(),
        testCase.getOutputRecords(),
        testCase.getInputRecords(),
        TestFunctionRegistry.INSTANCE.get(),
        new KsqlConfig(configs)
    );

    return allTopics.stream()
        .map(TopicNode::from)
        .collect(Collectors.toList());
  }

  private static TestInfoGatherer executeTestCaseAndGatherInfo(
      final TestCase testCase,
      final boolean validateResults
  ) {
    try (final TestExecutor testExecutor = TestExecutor.create(validateResults, Optional.empty())) {
      final TestInfoGatherer listener = new TestInfoGatherer();
      testExecutor.buildAndExecuteQuery(testCase, listener);
      return listener;
    } catch (final AssertionError | Exception e) {
      throw new AssertionError("Failed to run test case: " + e.getMessage()
          + System.lineSeparator()
          + "failed test: " + testCase.getName()
          + System.lineSeparator()
          + "in " + testCase.getTestLocation(),
          e
      );
    }
  }

  @VisibleForTesting
  static KsqlVersion getFormattedVersionFromPomFile() {
    try {
      final File pomFile = new File("pom.xml");
      final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
      final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      final Document pomDoc = documentBuilder.parse(pomFile);

      final NodeList versionNodeList = pomDoc.getElementsByTagName("version");
      final String versionName = versionNodeList.item(0).getTextContent();

      return KsqlVersion.parse(versionName.replaceAll("-SNAPSHOT?", ""));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestInfoGatherer implements TestExecutionListener {

    private final Builder<KsqlPlan> plansBuilder = new Builder<>();
    private PersistentQueryMetadata queryMetadata = null;
    private List<PostTopicNode> topics = ImmutableList.of();
    private List<SourceNode> sources = ImmutableList.of();

    @Override
    public void acceptPlan(final ConfiguredKsqlPlan plan) {
      plansBuilder.add(plan.getPlan());
    }

    @Override
    public void acceptQuery(final PersistentQueryMetadata query) {
      queryMetadata = query;
    }

    @Override
    public void runComplete(
        final List<PostTopicNode> knownTopics,
        final List<SourceNode> knownSources
    ) {
      if (queryMetadata == null) {
        throw new AssertionError("test case does not build a query");
      }

      this.topics = ImmutableList.copyOf(knownTopics);
      this.sources = ImmutableList.copyOf(knownSources);
    }

    public Map<String, SchemaNode> getSchemas() {
      return queryMetadata.getQuerySchemas().getLoggerSchemaInfo().entrySet().stream()
          .collect(Collectors.toMap(
              Entry::getKey,
              e -> SchemaNode.fromSchemaInfo(e.getValue())));
    }

    public List<PostTopicNode> getTopics() {
      return topics;
    }

    public List<SourceNode> getSources() {
      return sources;
    }

    public List<KsqlPlan> getPlans() {
      return plansBuilder.build();
    }

    public String getTopologyDescription() {
      return queryMetadata.getTopologyDescription();
    }
  }
}
