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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.TestExecutor;
import io.confluent.ksql.test.tools.TestExecutorUtil;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;;
import java.util.Collections;
import java.util.List;
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
  private static final ObjectMapper MAPPER = PlanJsonMapper.create();
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
    final KsqlConfig configs = BASE_CONFIG.cloneWithPropertyOverwrite(testCase.properties());
    try (
        final ServiceContext serviceContext = getServiceContext();
        final KsqlEngine engine = getKsqlEngine(serviceContext)) {
      return buildStatementsInTestCase(
          testCase,
          configs,
          serviceContext,
          engine,
          CURRENT_VERSION,
          System.currentTimeMillis()
      );
    }
  }

  /**
   * Rebuilds a TestCasePlan given a TestCase and a TestCasePlan
   * @param testCase the test case to rebuild the plan for
   * @param original the plan to rebuild
   * @return the rebuilt plan.
   */
  public static TestCasePlan rebuiltForTestCase(
      final TestCase testCase,
      final TestCasePlan original) {
    final TestCase withOriginal = PlannedTestUtils.buildPlannedTestCase(testCase, original);
    final KsqlConfig configs = BASE_CONFIG.cloneWithPropertyOverwrite(testCase.properties());
    try (
        final ServiceContext serviceContext = getServiceContext();
        final KsqlEngine engine = getKsqlEngine(serviceContext)) {
      return buildStatementsInTestCase(
          withOriginal,
          configs,
          serviceContext,
          engine,
          original.getVersion(),
          original.getTimestamp()
      );
    }
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
      final KsqlVersion version = KsqlVersion.parse(candidate.getVersion())
          .withTimestamp(candidate.getTimestamp());
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
    final PlannedTestPath specPath = versionDir.resolve(PlannedTestPath.SPEC_FILE);
    final PlannedTestPath topologyPath = versionDir.resolve(PlannedTestPath.TOPOLOGY_FILE);
    try {
      return new TestCasePlan(
          MAPPER.readValue(slurp(specPath), TestCasePlanNode.class),
          slurp(topologyPath)
      );
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String slurp(final PlannedTestPath path) throws IOException {
    return new String(
        Files.readAllBytes(path.relativePath()),
        Charset.defaultCharset()
    );
  }

  private static TestCasePlan buildStatementsInTestCase(
      final TestCase testCase,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final String version,
      final long timestamp) {
    final Iterable<ConfiguredKsqlPlan> configuredPlans = TestExecutorUtil.planTestCase(
        ksqlEngine,
        testCase,
        ksqlConfig,
        Optional.of(serviceContext.getSchemaRegistryClient()),
        KAFKA_STUB
    );
    final ImmutableList.Builder<KsqlPlan> plansBuilder = new Builder<>();
    PersistentQueryMetadata queryMetadata = null;
    for (final ConfiguredKsqlPlan configuredPlan : configuredPlans) {
      plansBuilder.add(configuredPlan.getPlan());
      final ExecuteResult executeResult = ksqlEngine.execute(
          ksqlEngine.getServiceContext(),
          configuredPlan
      );
      if (executeResult.getQuery().isPresent()) {
        queryMetadata = (PersistentQueryMetadata) executeResult.getQuery().get();
      }
    }
    if (queryMetadata == null) {
      throw new AssertionError("test case does not build a query");
    }
    return new TestCasePlan(
        version,
        timestamp,
        plansBuilder.build(),
        queryMetadata.getTopologyDescription(),
        queryMetadata.getSchemasDescription(),
        BASE_CONFIG.getAllConfigPropsWithSecretsObfuscated()
    );
  }

  private static ServiceContext getServiceContext() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    return TestServiceContext.create(() -> schemaRegistryClient);
  }

  private static KsqlEngine getKsqlEngine(final ServiceContext serviceContext) {
    final MutableMetaStore metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    return KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
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
}
