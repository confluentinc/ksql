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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Category({IntegrationTest.class})
public class InsertionIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(InsertionIntegrationTest.class);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private  static final String TOPIC_NAME = "sample_topic";
  private  static final String STREAM_NAME = "source_stream";

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,"http://foo:8080")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private KsqlRestClient ksqlRestClient;

  @Before
  public void setupRun() {
    ksqlRestClient = REST_APP.buildKsqlClient(Optional.empty());
  }

  @After
  public void afterRun() {
    TEST_HARNESS.deleteTopics(Arrays.asList(TOPIC_NAME));
  }

  @Test
  public void shouldAvoidNewSchemaRegistrationWithInsertValues() {
    ksqlRestClient.makeKsqlRequest
            ("CREATE STREAM " + STREAM_NAME + " (K0 INT KEY, K1 STRING KEY,V0 BOOLEAN, V1 INT) " +
                    "WITH (KAFKA_TOPIC = '" + TOPIC_NAME + "', VALUE_FORMAT = 'AVRO', " +
                    "KEY_FORMAT = 'AVRO', PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
            ("INSERT INTO " + STREAM_NAME + " (K0, K1, V0, V1) VALUES (1,'foo', true, 3);");

    String key = KsqlConstants.getSRSubject(TOPIC_NAME, true);
    String value = KsqlConstants.getSRSubject(TOPIC_NAME, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(key), is(1));
    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(1));
  }
}
