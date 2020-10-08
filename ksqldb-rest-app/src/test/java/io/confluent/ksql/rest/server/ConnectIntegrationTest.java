/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.WarningEntity;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class ConnectIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectIntegrationTest.class);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private static ConnectExecutable CONNECT;

  @BeforeClass
  public static void setUpClass() {
    CONNECT = new ConnectExecutable(ImmutableMap.<String, String>builder()
        .put("bootstrap.servers", TEST_HARNESS.kafkaBootstrapServers())
        .put("group.id", UUID.randomUUID().toString())
        .put("key.converter", StringConverter.class.getName())
        .put("value.converter", JsonConverter.class.getName())
        .put("offset.storage.topic", "connect-offsets")
        .put("status.storage.topic", "connect-status")
        .put("config.storage.topic", "connect-config")
        .put("offset.storage.replication.factor", "1")
        .put("status.storage.replication.factor", "1")
        .put("config.storage.replication.factor", "1")
        .build()
    );
    CONNECT.startAsync();
  }

  @AfterClass
  public static void tearDownClass() {
    CONNECT.shutdown();
  }

  private KsqlRestClient ksqlRestClient;
  private Set<String> connectNames;

  @Before
  public void setupRun() {
    ksqlRestClient = REST_APP.buildKsqlClient(Optional.empty());
    connectNames = new HashSet<>();
  }

  @After
  public void afterRun() {
    Iterators.consumingIterator(connectNames.iterator())
        .forEachRemaining(
            name -> ksqlRestClient.makeKsqlRequest("DROP CONNECTOR `" + name + "`;"));

    assertThatEventually(
        () -> ((ConnectorList) ksqlRestClient.makeKsqlRequest("SHOW CONNECTORS;").getResponse()
            .get(0)).getConnectors(),
        Matchers.empty()
    );
  }

  @Test
  public void shouldListConnectors() {
    // Given:
    create("mock-connector", ImmutableMap.of(
        "connector.class", "org.apache.kafka.connect.tools.MockSourceConnector"
    ));

    // When:
    final RestResponse<KsqlEntityList> response = ksqlRestClient
        .makeKsqlRequest("SHOW CONNECTORS;");

    // Then:
    assertThat("expected successful response", response.isSuccessful());
    assertThat(response.getResponse(), hasSize(1));
    assertThat(response.getResponse().get(0), instanceOf(ConnectorList.class));
    assertThat(((ConnectorList) response.getResponse().get(0)).getConnectors().size(), is(1));
    assertThat(
        ((ConnectorList) response.getResponse().get(0)).getConnectors().get(0).getName(),
        is("mock-connector"));
    assertThatEventually(
        () -> ((ConnectorList) ksqlRestClient.makeKsqlRequest("SHOW CONNECTORS;").getResponse()
            .get(0)).getConnectors().get(0).getState(),
        is("RUNNING (1/1 tasks RUNNING)"));
  }

  @Test
  public void shouldDescribeConnector() {
    // Given:
    create("mock-connector", ImmutableMap.of(
        "connector.class", "org.apache.kafka.connect.tools.MockSourceConnector"
    ));

    // When:
    final AtomicReference<RestResponse<KsqlEntityList>> responseHolder = new AtomicReference<>();
    assertThatEventually(
        () -> {
          responseHolder
              .set(ksqlRestClient.makeKsqlRequest("DESCRIBE CONNECTOR `mock-connector`;"));
          return responseHolder.get().getResponse().get(0);
        },
        // there is a race condition were create from line 150 may not have gone through
        instanceOf(ConnectorDescription.class)
    );
    final RestResponse<KsqlEntityList> response = responseHolder.get();

    // Then:
    assertThat("expected successful response", response.isSuccessful());
    assertThat(response.getResponse(), hasSize(1));
    assertThat(response.getResponse().get(0), instanceOf(ConnectorDescription.class));
    assertThat(
        ((ConnectorDescription) response.getResponse().get(0)).getConnectorClass(),
        is("org.apache.kafka.connect.tools.MockSourceConnector"));
    assertThat(
        ((ConnectorDescription) response.getResponse().get(0)).getStatus().name(),
        is("mock-connector"));
  }

  @Test
  public void shouldDropConnector() {
    // Given:
    create("mock-connector", ImmutableMap.of(
        "connector.class", "org.apache.kafka.connect.tools.MockSourceConnector"
    ));

    // When:
    final RestResponse<KsqlEntityList> response = ksqlRestClient
        .makeKsqlRequest("DROP CONNECTOR `mock-connector`;");

    // Then:
    assertThat("expected successful response", response.isSuccessful());
    assertThat(response.getResponse(), hasSize(1));
    assertThat(response.getResponse().get(0), instanceOf(DropConnectorEntity.class));
    assertThat(
        ((DropConnectorEntity) response.getResponse().get(0)).getConnectorName(),
        is("mock-connector"));
  }

  @Test
  public void shouldReturnWarning() {
    // Given:
    create("mock-connector", ImmutableMap.of(
        "connector.class", "org.apache.kafka.connect.tools.MockSourceConnector"
    ));

    // When:
    final RestResponse<KsqlEntityList> response = ksqlRestClient
        .makeKsqlRequest("CREATE SOURCE CONNECTOR IF NOT EXISTS `mock-connector` "
            + "WITH(\"connector.class\"='org.apache.kafka.connect.tools.MockSourceConnector');");

    //Then
    assertThat("expected successful response", response.isSuccessful());
    assertThat(response.getResponse().get(0), instanceOf(WarningEntity.class));
    assertThat(((WarningEntity) response.getResponse().get(0)).getMessage(),
        equalToIgnoringCase("Connector mock-connector already exists"));
  }

  @Test
  public void shouldReturnError() {
    // Given:
    create("mock-connector", ImmutableMap.of(
        "connector.class", "org.apache.kafka.connect.tools.MockSourceConnector"
    ));

    // When:
    final RestResponse<KsqlEntityList> response = ksqlRestClient
        .makeKsqlRequest("CREATE SOURCE CONNECTOR `mock-connector` "
            + "WITH(\"connector.class\"='org.apache.kafka.connect.tools.MockSourceConnector');");

    //Then
    assertThat("expected successful response", response.isSuccessful());
    assertThat(response.getResponse().get(0), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) response.getResponse().get(0)).getErrorMessage(),
        containsString("Connector mock-connector already exists"));
  }

  private void create(final String name, final Map<String, String> properties) {
    connectNames.add(name);
    final String withClause = Joiner.on(",")
        .withKeyValueSeparator("=")
        .join(properties.entrySet().stream().collect(Collectors.toMap(
            e -> "\"" + e.getKey() + "\"",
            e -> "'" + e.getValue() + "'")));

    final RestResponse<KsqlEntityList> response = ksqlRestClient.makeKsqlRequest(
        "CREATE SOURCE CONNECTOR `" + name + "` WITH(" + withClause + ");");
    LOG.info("Got response from Connect: {}", response);
  }

}
