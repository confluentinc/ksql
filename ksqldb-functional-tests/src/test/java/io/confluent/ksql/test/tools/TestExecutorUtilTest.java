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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.QttTestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.tools.TestExecutorUtil.PlannedStatement;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.tools.test.model.TestLocation;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import java.io.File;
import java.util.Iterator;
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

  private static final File TEST_FILE = new File("src/test/resources/testing_tool_tests.json");
  private static final String PROJECT_AND_FILTER = "project and filter";
  private static final String FIRST_STATEMENT_FAILS = "invalid: first statement fails";

  @Mock
  private TestLocation location;
  @Mock
  private TestExecutionListener listener;

  private ServiceContext serviceContext;
  private KsqlEngine ksqlEngine;
  private KsqlConfig ksqlConfig;
  private StubKafkaService stubKafkaService;

  @Before
  public void setUp() {
    serviceContext = TestExecutor.getServiceContext();
    ksqlEngine = TestExecutor.getKsqlEngine(serviceContext, Optional.empty());
    ksqlConfig = new KsqlConfig(TestExecutor.baseConfig());
    stubKafkaService = StubKafkaService.create();

    stubKafkaService.ensureTopic(new Topic("test_topic", Optional.empty(), Optional.empty()));
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldPlanTestCase() {
    // Given:
    final TestCase testCase = loadTestCase(PROJECT_AND_FILTER);

    // When:
    final Iterator<PlannedStatement> plans = TestExecutorUtil.planTestCase(
        ksqlEngine,
        testCase,
        ksqlConfig,
        serviceContext,
        Optional.of(serviceContext.getSchemaRegistryClient()),
        stubKafkaService
    );

    // Then:
    final List<ConfiguredKsqlPlan> asList = new LinkedList<>();
    while (plans.hasNext()) {
      final PlannedStatement planned = plans.next();
      final ConfiguredKsqlPlan plan = planned.plan
          .orElseThrow(() -> new AssertionError("Should be plan"));
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
    final TestCase testCase = loadTestCase(PROJECT_AND_FILTER);

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
    final TopologyTestDriverContainer topologyTestDriverContainer = topologyTestDriverContainerList
        .get(0);
    assertThat(topologyTestDriverContainer.getSourceTopicNames().size(), equalTo(1));
    assertThat(topologyTestDriverContainer.getSourceTopicNames().iterator().next(),
        equalTo("test_topic"));
    assertThat(topologyTestDriverContainer.getSinkTopicName(), equalTo(Optional.of("S1")));
    assertThat(topologyTestDriverContainer.getTopologyTestDriver(), notNullValue());
  }

  @Test
  public void shouldNotThrowFromHasNextWhenNextStatementWillFail() {
    // Given:
    final TestCase testCase = loadTestCase(FIRST_STATEMENT_FAILS);

    final Iterator<PlannedStatement> plans = TestExecutorUtil.planTestCase(
        ksqlEngine,
        testCase,
        ksqlConfig,
        serviceContext,
        Optional.of(serviceContext.getSchemaRegistryClient()),
        stubKafkaService
    );

    // When:
    final boolean result = plans.hasNext();

    // Then (did not throw):
    assertThat("should have next", result);
  }

  @Test
  public void shouldThrowOnNextIfStatementFails() {
    // Given:
    final TestCase testCase = loadTestCase(FIRST_STATEMENT_FAILS);

    final Iterator<PlannedStatement> plans = TestExecutorUtil.planTestCase(
        ksqlEngine,
        testCase,
        ksqlConfig,
        serviceContext,
        Optional.of(serviceContext.getSchemaRegistryClient()),
        stubKafkaService
    );

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        plans::next
    );

    // Then:
    assertThat(e.getMessage(), containsString("UNKNOWN_SOURCE does not exist"));
  }

  private static TestCase loadTestCase(final String testName) {
    try {
      final QttTestFile qttTestFile = TestJsonMapper.INSTANCE.get()
          .readValue(TEST_FILE, QttTestFile.class);

      final TestCaseNode testCaseNode = qttTestFile.tests.stream()
          .filter(tcn -> tcn.name().equals(testName))
          .findFirst()
          .orElseThrow(() ->
              new AssertionError("Invalid test: no test case named " + testName));

      return TestCaseBuilder.buildTests(
          testCaseNode,
          TEST_FILE.toPath(),
          name -> mock(TestLocation.class)
      ).get(0);
    } catch (final Exception e) {
      throw new AssertionError("Invalid test: failed to load test " + testName, e);
    }
  }
}