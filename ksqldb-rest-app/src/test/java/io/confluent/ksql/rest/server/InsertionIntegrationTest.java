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
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
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
  private  static final String SIMPLE_TOPIC = "simple_topic";
  private  static final String EVOLVING_TOPIC = "evolving_topic";
  private  static final String BASE_TOPIC = "base_topic";


  private  static final String SIMPLE_STREAM = "simple_stream";
  private  static final String DECIMAL_STREAM = "decimal_stream";
  private  static final String MAP_STREAM = "map_stream";
  private  static final String ARRAY_STREAM = "array_stream";
  private  static final String STRUCT_STREAM = "struct_stream";
  private  static final String STRUCT_BASE_STREAM = "base_stream";
  private  static final String FOLLOWER_STREAM = "follower_stream";

  private  static final String EVOLVING_STREAM = "evolving_stream";

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,"http://foo:8080")
      .withProperty(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
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

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + SIMPLE_STREAM + " (K0 INT KEY, K1 STRING KEY,V0 BOOLEAN, V1 INT) " +
            "WITH (KAFKA_TOPIC = '" + SIMPLE_TOPIC + "', VALUE_FORMAT = 'AVRO', " +
            "KEY_FORMAT = 'AVRO', PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + DECIMAL_STREAM + " (K0 INT KEY, V0 DECIMAL(21,19)) " +
            "WITH (KAFKA_TOPIC = '" + DECIMAL_STREAM + "', VALUE_FORMAT = 'AVRO', " +
            "KEY_FORMAT = 'AVRO', PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + MAP_STREAM + " (K0 INT KEY, V0 MAP<STRING, INT>) " +
            "WITH (KAFKA_TOPIC = '" + MAP_STREAM + "', VALUE_FORMAT = 'AVRO', " +
            "KEY_FORMAT = 'AVRO', PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + ARRAY_STREAM + " (K0 INT KEY, V0 ARRAY<INT>) " +
            "WITH (KAFKA_TOPIC = '" + ARRAY_STREAM + "', VALUE_FORMAT = 'AVRO', " +
            "KEY_FORMAT = 'AVRO', PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + STRUCT_STREAM + " (K0 INT KEY, V0 STRUCT<A INT, B STRING, C BOOLEAN>) " +
            "WITH (KAFKA_TOPIC = '" + STRUCT_STREAM + "', VALUE_FORMAT = 'AVRO', " +
            "KEY_FORMAT = 'AVRO', PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + EVOLVING_STREAM + " (K0 INT KEY, V0 BOOLEAN) " +
            "WITH (KAFKA_TOPIC = '" + EVOLVING_TOPIC + "', VALUE_FORMAT = 'AVRO', " +
            "PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + STRUCT_BASE_STREAM + " (K0 INT KEY, "
            + "V0 STRUCT<A INT, B STRING, C BOOLEAN>, V1 ARRAY<INT>, V2 STRING) "
            + "WITH (KAFKA_TOPIC = '" + BASE_TOPIC + "', VALUE_FORMAT = 'AVRO',  PARTITIONS = 1);");
  }

  @After
  public void afterRun() {
  }

  @Test
  public void shouldAvoidNewSchemaRegistrationForCompoundKeyWithInsertValues() {

    ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + SIMPLE_STREAM + " (K0, K1, V0, V1) VALUES (1,'foo', true, 3);");

    String key = KsqlConstants.getSRSubject(SIMPLE_TOPIC, true);
    String value = KsqlConstants.getSRSubject(SIMPLE_TOPIC, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(key), is(1));
    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(1));
  }

  @Test
  public void shouldAvoidNewSchemaRegistrationForDecimalWithInsertValues() {

    ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + DECIMAL_STREAM + " (K0, V0) VALUES (1, 14.75);");

    String key = KsqlConstants.getSRSubject(DECIMAL_STREAM, true);
    String value = KsqlConstants.getSRSubject(DECIMAL_STREAM, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(key), is(1));
    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(1));
  }

  @Test
  public void shouldAvoidNewSchemaRegistrationForMapWithInsertValues() {

    ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + MAP_STREAM + " (K0, V0) VALUES (1, MAP<\"foo\", 10>);");

    String key = KsqlConstants.getSRSubject(MAP_STREAM, true);
    String value = KsqlConstants.getSRSubject(MAP_STREAM, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(key), is(1));
    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(1));
  }

  @Test
  public void shouldAvoidNewSchemaRegistrationForArrayWithInsertValues() {

    ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + ARRAY_STREAM + " (K0, V0) VALUES (1, Array[10, 20, 30]);");

    String key = KsqlConstants.getSRSubject(ARRAY_STREAM, true);
    String value = KsqlConstants.getSRSubject(ARRAY_STREAM, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(key), is(1));
    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(1));
  }

  @Test
  public void shouldAvoidNewSchemaRegistrationForStructWithInsertValues() {

    ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + STRUCT_STREAM + " (K0, V0) VALUES (1, STRUCT<10, \"foo\", true>);");

    String key = KsqlConstants.getSRSubject(STRUCT_STREAM, true);
    String value = KsqlConstants.getSRSubject(STRUCT_STREAM, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(key), is(1));
    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(1));
  }
  @Test
  public void shouldInsertValuesToStreamsCreatedWithSchemaID() {

    String value = KsqlConstants.getSRSubject(BASE_TOPIC, false);

    ksqlRestClient.makeKsqlRequest
        ("CREATE STREAM " + FOLLOWER_STREAM + " WITH (KAFKA_TOPIC = '" + BASE_TOPIC + "',"
            + " KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'AVRO',"
            + " VALUE_SCHEMA_ID = " + TEST_HARNESS.getLatestSchemaID(value) + ", PARTITIONS = 1);");

    RestResponse<KsqlEntityList> response =  ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + FOLLOWER_STREAM + " (V0, V1, V2) VALUES "
            + "(STRUCT(A := 10, B := 'foo', C := true), ARRAY[1,2,3], 'bar');");

    assertThat(response.getStatusCode(), is(200));
    assertThat(response.isSuccessful(), is(true));
  }
  @Test
  public void shouldSupportEvolvingSchemasOnInsert() {

    ksqlRestClient.makeKsqlRequest
        ("CREATE OR REPLACE STREAM " + EVOLVING_STREAM + " (K0 INT KEY, V0 BOOLEAN, V1 STRING) " +
            "WITH (KAFKA_TOPIC = '" + EVOLVING_TOPIC + "', VALUE_FORMAT = 'AVRO', " +
            "PARTITIONS = 1);");

    ksqlRestClient.makeKsqlRequest
        ("INSERT INTO " + EVOLVING_STREAM + " (K0, V0, V1) VALUES (1, true, 'foo');");

    String value = KsqlConstants.getSRSubject(EVOLVING_TOPIC, false);

    assertThat(TEST_HARNESS.getLatestSchemaVersion(value), is(2));
  }
}
