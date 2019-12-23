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
import java.util.List;
import java.util.Optional;
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
  public static TestCasePlan fromTestCase(final TestCase testCase) {
    final KsqlConfig configs = BASE_CONFIG.cloneWithPropertyOverwrite(testCase.properties());
    try (
        final ServiceContext serviceContext = getServiceContext();
        final KsqlEngine engine = getKsqlEngine(serviceContext)) {
      return buildStatementsInTestCase(testCase, configs, serviceContext, engine);
    }
  }

  /**
   * Create a TestCasePlan by loading it from the local filesystem. This factory loads the
   * most recent plan from a given test case directory.
   * @param testCaseDir The directory to load the plan from.
   * @return the loaded plan.
   */
  public static Optional<TestCasePlan> fromLatest(final Path testCaseDir) {
    final Optional<List<String>> existing = PlannedTestUtils.loadContents(testCaseDir.toString());
    if (!existing.isPresent()) {
      return Optional.empty();
    }
    KsqlVersion latestVersion = null;
    TestCasePlan latest = null;
    for (final String versionDir : existing.get()) {
      final TestCasePlan planAtVersionNode = parseSpec(testCaseDir.resolve(versionDir));
      final KsqlVersion version = KsqlVersion.parse(planAtVersionNode.getVersion())
          .withTimestamp(planAtVersionNode.getTimestamp());
      if (latestVersion == null || latestVersion.compareTo(version) < 0) {
        latestVersion = version;
        latest = planAtVersionNode;
      }
    }
    return Optional.ofNullable(latest);
  }

  /**
   * Create a TestCasePlan by loading a specific plan from the local filesystem.
   * @param versionDir the directory to load the plan from.
   * @return the loaded plan.
   */
  public static TestCasePlan fromSpecific(final Path versionDir) {
    return parseSpec(versionDir);
  }

  private static TestCasePlan parseSpec(final Path versionDir) {
    final Path specPath = versionDir.resolve(PlannedTestLoader.SPEC_FILE);
    final Path topologyPath = versionDir.resolve(PlannedTestLoader.TOPOLOGY_FILE);
    try {
      return new TestCasePlan(
          MAPPER.readValue(slurp(specPath), TestCasePlanNode.class),
          slurp(topologyPath)
      );
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String slurp(final Path path) throws IOException {
    return new String(
        Files.readAllBytes(PlannedTestUtils.findBaseDir().resolve(path)),
        Charset.defaultCharset()
    );
  }

  private static TestCasePlan buildStatementsInTestCase(
      final TestCase testCase,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine) {
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
        CURRENT_VERSION,
        System.currentTimeMillis(),
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
