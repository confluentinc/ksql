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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.QttTestFile;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestExecutorUtilTest {

  private ServiceContext serviceContext;
  private KsqlEngine ksqlEngine;
  private KsqlConfig ksqlConfig;
  private TestCase testCase;
  private FakeKafkaService fakeKafkaService;

  @Before
  public void setUp() throws IOException {
    final QttTestFile qttTestFile = JsonMapper.INSTANCE.mapper
        .readValue(new File("src/test/resources/testing_tool_tests.json"), QttTestFile.class);
    final TestCaseNode testCaseNode = qttTestFile.tests.get(0);
    testCase = testCaseNode.buildTests(
        new File("src/test/resources/testing_tool_tests.json").toPath(),
        TestFunctionRegistry.INSTANCE.get()
    ).get(0);

    serviceContext = TestExecutor.getServiceContext();
    ksqlEngine = TestExecutor.getKsqlEngine(serviceContext);
    ksqlConfig = new KsqlConfig(TestExecutor.getConfigs(Collections.emptyMap()));
    fakeKafkaService = FakeKafkaService.create();
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldBuildStreamsTopologyTestDrivers() throws IOException, RestClientException {

    // Given:
    final Topic sourceTopic = new Topic(
        "test_topic",
        Optional.empty(),
        new StringSerdeSupplier(),
        new StringSerdeSupplier(),
        1,
        1,
        Optional.empty()
    );

    fakeKafkaService.createTopic(sourceTopic);

    // When:
    final List<TopologyTestDriverContainer> topologyTestDriverContainerList = TestExecutorUtil.buildStreamsTopologyTestDrivers(
        testCase,
        serviceContext,
        ksqlEngine,
        ksqlConfig,
        fakeKafkaService
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