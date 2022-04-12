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
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThrows;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.util.KsqlConfig;
import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.storage.StringConverter;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class ConnectIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectIntegrationTest.class);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private  static final long TIMEOUT_NS = 120000000000L;

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
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
        .put("value.converter.schemas.enable", "false")
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
  public void afterRun() throws UnsupportedEncodingException {
    Iterators.consumingIterator(connectNames.iterator())
        .forEachRemaining(
            name -> ksqlRestClient.makeKsqlRequest("DROP CONNECTOR `" + name + "`;"));

    assertThatEventually(
        () -> ((ConnectorList) ksqlRestClient.makeKsqlRequest("SHOW CONNECTORS;").getResponse()
            .get(0)).getConnectors(),
        Matchers.empty()
    );

    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8"));
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

    //Then:
    assertThat("expected error response", response.isErroneous());
    final KsqlErrorMessage err = response.getErrorMessage();
    assertThat(err.getErrorCode(), is(Errors.toErrorCode(HttpStatus.SC_CONFLICT)));
    assertThat(err.getMessage(), containsString("Connector mock-connector already exists"));
  }

  @Test
  public void shouldReadTimeTypesAndHeadersFromConnect() {
    // Given:
    create("mock-source", ImmutableMap.<String, String> builder()
        .put("connector.class", "org.apache.kafka.connect.tools.VerifiableSourceConnector")
        .put("topic", "foo")
        .put("throughput", "5")
        .put("id", "123")
        .put("topic.creation.default.replication.factor", "1")
        .put("topic.creation.default.partitions", "1")
        .build());

    makeKsqlRequest("CREATE STREAM TIMESTAMP_STREAM (PAYLOAD TIMESTAMP, H ARRAY<STRUCT<key STRING, value BYTES>> HEADERS) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='DELIMITED');");
    makeKsqlRequest("CREATE STREAM TIME_STREAM (PAYLOAD TIME) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='DELIMITED');");
    makeKsqlRequest("CREATE STREAM DATE_STREAM (PAYLOAD DATE) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='DELIMITED');");

    // When:
    final RestResponse<List<StreamedRow>> queryTimestamp = ksqlRestClient.makeQueryRequest("SELECT * FROM TIMESTAMP_STREAM EMIT CHANGES LIMIT 1;", 1L);
    final RestResponse<List<StreamedRow>> queryTime = ksqlRestClient.makeQueryRequest("SELECT * FROM TIME_STREAM EMIT CHANGES LIMIT 1;", 1L);
    final RestResponse<List<StreamedRow>> queryDate = ksqlRestClient.makeQueryRequest("SELECT * FROM DATE_STREAM EMIT CHANGES LIMIT 1;", 1L);

    // Then:
    assertThat("successfully queried TIMESTAMP_STREAM", queryTimestamp.isSuccessful());
    assertThat("successfully queried TIME_STREAM", queryTime.isSuccessful());
    assertThat("successfully queried DATE_STREAM", queryDate.isSuccessful());
    assertThat(queryTimestamp.getResponse().get(1).getRow().get().getColumns().get(0), is("1970-01-01T00:00:00.000"));
    assertThat(queryTimestamp.getResponse().get(1).getRow().get().getColumns().get(1), is(
        ImmutableList.of()));
    assertThat(queryTime.getResponse().get(1).getRow().get().getColumns().get(0), is("00:00"));
    assertThat(queryDate.getResponse().get(1).getRow().get().getColumns().get(0), is("1970-01-01"));
  }

  @Test
  @Ignore
  public void shouldWriteTimestampsToConnect() throws UnsupportedEncodingException {
    // Given:
    ksqlRestClient.makeKsqlRequest("CREATE STREAM BAR (PAYLOAD TIMESTAMP) WITH (KAFKA_TOPIC='bar', value_format='JSON', PARTITIONS=1);");
    ksqlRestClient.makeKsqlRequest("INSERT INTO BAR VALUES ('1970-01-01T00:00:00');");

    // When:
    create("mock-sink", ImmutableMap.<String, String> builder()
        .put("connector.class", "org.apache.kafka.connect.tools.VerifiableSinkConnector")
        .put("topics", "BAR")
        .put("id", "456")
        .put("value.converter.schemas.enable", "false")
        .build(), ConnectorType.SINK);

    // Then:
    Iterable<String> sinkOutputParts = Arrays.asList(
        "{\"task\":null,\"seqno\":{\"PAYLOAD\":0},\"offset\":0,\"time_ms\":",
        ",\"name\":\"mock-sink\",\"topic\":\"BAR\""
    );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out, true, "UTF-8"));
    assertThatEventually("checks the values received by the connector", () -> {
          try {
            return out.toString("UTF-8");
          } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
          }
        },
        stringContainsInOrder(sinkOutputParts), TIMEOUT_NS, TimeUnit.NANOSECONDS);
  }

  private void create(final String name, final Map<String, String> properties) {
    create(name, properties, ConnectorType.SOURCE);
  }

  private void create(final String name, final Map<String, String> properties, ConnectorType type) {
    connectNames.add(name);
    final String withClause = Joiner.on(",")
        .withKeyValueSeparator("=")
        .join(properties.entrySet().stream().collect(Collectors.toMap(
            e -> "\"" + e.getKey() + "\"",
            e -> "'" + e.getValue() + "'")));

    final RestResponse<KsqlEntityList> response = ksqlRestClient.makeKsqlRequest(
        "CREATE " + type + " CONNECTOR `" + name + "` WITH(" + withClause + ");");
    LOG.info("Got response from Connect: {}", response);
  }

  private void makeKsqlRequest(final String request) {
    final long start = System.nanoTime();
    RestResponse<KsqlEntityList> response;
    do {
      response = ksqlRestClient.makeKsqlRequest(request);
    } while(!response.isSuccessful() && System.nanoTime() - start < TIMEOUT_NS);
  }
}
