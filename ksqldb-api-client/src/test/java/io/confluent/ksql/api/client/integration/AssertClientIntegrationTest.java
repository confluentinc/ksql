/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.client.integration;

import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class AssertClientIntegrationTest {

  private static final PhysicalSchema SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("K"), SqlTypes.INTEGER)
          .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
          .build(),
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty(KSQL_HEADERS_COLUMNS_ENABLED, true)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics("abc");
    // registers a schema under the subject name "abc-value"
    TEST_HARNESS.ensureSchema("abc", SCHEMA, false);
  }

  private Vertx vertx;
  private Client client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    client = createClient();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @Test
  public void shouldThrowOnAssertNonexistentSchema() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.assertSchema("NONEXISTENT", 3, true, Duration.ofSeconds(3)).get()
    );

    // Then:
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 417 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Schema with subject name NONEXISTENT id 3 does not exist"));
    assertThat(e.getCause().getMessage(), containsString("Error code: 41700"));
  }

  @Test
  public void shouldThrowOnAssertNotExistsNonExistingSchema() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.assertSchema("abc-value", 1, false, Duration.ofSeconds(3)).get()
    );

    // Then:
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 417 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Schema with subject name abc-value id 1 exists."));
    assertThat(e.getCause().getMessage(), containsString("Error code: 41700"));
  }

  @Test
  public void shouldAssertSchema() throws ExecutionException, InterruptedException {
    client.assertSchema("abc-value", true).get();
    client.assertSchema("abc-value", 1, true).get();
    client.assertSchema(1, true, Duration.ofSeconds(3)).get();
  }

  @Test
  public void shouldAssertNotExistsSchema() throws ExecutionException, InterruptedException {
    client.assertSchema(34, false).get();
    client.assertSchema("NONEXISTENT", false, Duration.ofSeconds(3)).get();
    client.assertSchema("NONEXISTENT", 43, false, Duration.ofSeconds(3)).get();
  }

  @Test
  public void shouldThrowOnAssertNonexistentTopic() {
    // When
    final Exception e1 = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.assertTopic("NONEXISTENT", true).get()
    );
    final Exception e2 = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.assertTopic("abc", ImmutableMap.of("partitions", 7, "foo", 3), true).get()
    );

    // Then:
    assertThat(e1.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e1.getCause().getMessage(), containsString("Received 417 response from server"));
    assertThat(e1.getCause().getMessage(), containsString("Topic NONEXISTENT does not exist."));
    assertThat(e1.getCause().getMessage(), containsString("Error code: 41700"));

    assertThat(e2.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e2.getCause().getMessage(), containsString("Received 417 response from server"));
    assertThat(e2.getCause().getMessage(), containsString("Mismatched configuration for topic abc: For config partitions, expected 7 got 1"));
    assertThat(e2.getCause().getMessage(), containsString("Cannot assert unknown topic property: FOO."));
    assertThat(e2.getCause().getMessage(), containsString("Error code: 41700"));
  }

  @Test
  public void shouldThrowOnAssertNotExistsNonExistingTopic() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.assertTopic("abc", false, Duration.ofSeconds(3)).get()
    );

    // Then:
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 417 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Topic abc exists"));
    assertThat(e.getCause().getMessage(), containsString("Error code: 41700"));
  }

  @Test
  public void shouldAssertTopic() throws ExecutionException, InterruptedException {
    // Given:
    client.define("name", "abc");

    // These should run without throwing any errors
    client.assertTopic("abc", true).get();
    client.assertTopic("${name}", ImmutableMap.of("replicas", 1, "partitions", 1), true, Duration.ofSeconds(3)).get();
  }

  @Test
  public void shouldAssertNotExistsTopic() throws ExecutionException, InterruptedException {
    // These should run without throwing any errors
    client.assertTopic("NONEXISTENT", ImmutableMap.of("replicas", 1, "partitions", 1), false).get();
    client.assertTopic("NONEXISTENT", false, Duration.ofSeconds(3)).get();
  }

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    return Client.create(clientOptions, vertx);
  }
}
