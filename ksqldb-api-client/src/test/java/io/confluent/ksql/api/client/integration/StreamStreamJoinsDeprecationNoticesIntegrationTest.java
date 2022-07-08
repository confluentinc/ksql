/*
 * Copyright 2020 Confluent Inc.
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

import com.google.common.collect.ImmutableListMultimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@Category(IntegrationTest.class)
public class StreamStreamJoinsDeprecationNoticesIntegrationTest {
  private static final StructuredTypesDataProvider STRUCTURED_DATA_PROVIDER = new StructuredTypesDataProvider();

  private static final TestDataProvider STREAM_DATA_PROVIDER_1 = new TestDataProvider(
      "STRUCTURED_TYPES_1", STRUCTURED_DATA_PROVIDER.schema(), ImmutableListMultimap.of());

  private static final TestDataProvider STREAM_DATA_PROVIDER_2 = new TestDataProvider(
      "STRUCTURED_TYPES_2", STRUCTURED_DATA_PROVIDER.schema(), ImmutableListMultimap.of());

  private static final String STREAM_TOPIC_1 = STREAM_DATA_PROVIDER_1.topicName();
  private static final String STREAM_TOPIC_2 = STREAM_DATA_PROVIDER_2.topicName();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty(KSQL_HEADERS_COLUMNS_ENABLED, true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(STREAM_TOPIC_1, STREAM_TOPIC_2);
    RestIntegrationTestUtil.createStream(REST_APP, STREAM_DATA_PROVIDER_1);
    RestIntegrationTestUtil.createStream(REST_APP, STREAM_DATA_PROVIDER_2);
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  @After
  public void tearDown() {
    makeKsqlRequest("DROP STREAM DEPRECATED_QUERY;");
  }

  @Test
  public void shouldReturnDeprecationWarningMessageOnDeprecatedStatements() {
    // When
    final KsqlEntity ksqlEntity = makeKsqlRequest(String.format(
        "CREATE STREAM DEPRECATED_QUERY AS " +
            "SELECT * FROM %s AS l "
            + "LEFT JOIN %s AS r WITHIN 1 SECOND ON l.K->F1 = r.K->F1;",
        STREAM_DATA_PROVIDER_1.sourceName(),
        STREAM_DATA_PROVIDER_2.sourceName()
    )).get(0);

    // Then
    final CommandStatusEntity commandStatusEntity = (CommandStatusEntity) ksqlEntity;
    final CommandStatus commandStatus = commandStatusEntity.getCommandStatus();
    assertThat(commandStatus.getStatus(), is(CommandStatus.Status.SUCCESS));
    assertThat(commandStatus.getMessage(), containsString("Created query with ID CSAS_DEPRECATED_QUERY_"));
    assertThat(commandStatusEntity.getWarnings(), contains(
        new KsqlWarning(
            "DEPRECATION NOTICE: Stream-stream joins statements without a GRACE PERIOD "
                + "will not be accepted in a future ksqlDB version.\n"
                + "Please use the GRACE PERIOD clause as specified in "
                + "https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/"
                + "select-push-query/")
    ));
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }
}
