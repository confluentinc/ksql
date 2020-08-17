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

package io.confluent.ksql.test.tools;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.QttTestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.model.TestLocation;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestExecutorUtilTest {

  @Mock
  private TestLocation location;
  private ServiceContext serviceContext;
  private KsqlEngine ksqlEngine;
  private KsqlConfig ksqlConfig;
  private TestCase testCase;
  private StubKafkaService stubKafkaService;

  @Mock
  private TestExecutionListener listener;

  @Before
  public void setUp() throws IOException {
    final QttTestFile qttTestFile = TestJsonMapper.INSTANCE.get()
        .readValue(new File("src/test/resources/testing_tool_tests.json"), QttTestFile.class);
    final TestCaseNode testCaseNode = qttTestFile.tests.get(0);
    testCase = TestCaseBuilder.buildTests(testCaseNode,
        Paths.get("src/test/resources/testing_tool_tests.json"),
        testName -> location).get(0);

    serviceContext = TestExecutor.getServiceContext();
    ksqlEngine = TestExecutor.getKsqlEngine(serviceContext, Optional.empty());
    ksqlConfig = new KsqlConfig(TestExecutor.baseConfig());
    stubKafkaService = StubKafkaService.create();
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldPlanTestCase() {
    // Given:
    final Topic sourceTopic = new Topic("test_topic", Optional.empty());

    stubKafkaService.ensureTopic(sourceTopic);

    // When:
    final Iterable<ConfiguredKsqlPlan> plans = TestExecutorUtil.planTestCase(
        ksqlEngine,
        testCase,
        ksqlConfig,
        Optional.of(serviceContext.getSchemaRegistryClient()),
        stubKafkaService
    );

    // Then:
    final List<ConfiguredKsqlPlan> asList = new LinkedList<>();
    for (final ConfiguredKsqlPlan plan : plans) {
      ksqlEngine.execute(ksqlEngine.getServiceContext(), plan);
      asList.add(plan);
    }
    assertThat(asList.size(), is(2));
    assertThat(
        asList.get(0).getPlan().getStatementText(),
        startsWith("CREATE STREAM TEST")
    );
    assertThat(
        asList.get(1).getPlan().getStatementText(),
        startsWith("CREATE STREAM S1 AS SELECT")
    );
  }

  @Test
  public void shouldBuildStreamsTopologyTestDrivers() {
    // Given:
    final Topic sourceTopic = new Topic("test_topic", Optional.empty());

    stubKafkaService.ensureTopic(sourceTopic);

    // When:
    final List<TopologyTestDriverContainer> topologyTestDriverContainerList =
        TestExecutorUtil.buildStreamsTopologyTestDrivers(
            testCase,
            serviceContext,
            ksqlEngine,
            ksqlConfig,
            stubKafkaService,
            listener
        );

    // Then:
    assertThat(topologyTestDriverContainerList.size(), equalTo(1));
    final TopologyTestDriverContainer topologyTestDriverContainer = topologyTestDriverContainerList.get(0);
    assertThat(topologyTestDriverContainer.getSourceTopics().size(), equalTo(1));
    assertThat(topologyTestDriverContainer.getSourceTopics().iterator().next().getName(), equalTo("test_topic"));
    assertThat(topologyTestDriverContainer.getSinkTopic().getName(), equalTo("S1"));
    assertThat(topologyTestDriverContainer.getTopologyTestDriver(), notNullValue());
  }
}