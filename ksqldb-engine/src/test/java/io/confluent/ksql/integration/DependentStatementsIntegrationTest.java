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
package io.confluent.ksql.integration;

import static java.lang.String.format;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class DependentStatementsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(DependentStatementsIntegrationTest.class);

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  public TestKsqlContext ksqlContext;

  @Rule
  public final Timeout timeout = Timeout.seconds(120);

  private final List<QueryMetadata> toClose = new ArrayList<>();

  @Before
  public void before() throws Exception {
    TEST_HARNESS.before();
    ksqlContext  = TEST_HARNESS.ksqlContextBuilder()
        .withAdditionalConfig(
            KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
            "http://foo:8080")
        .build();

    ksqlContext.before();

    toClose.clear();
  }

  @After
  public void after() {
    toClose.forEach(QueryMetadata::close);
    ksqlContext.after();
    TEST_HARNESS.after();
  }

  @Test
  public void shouldCreateDependentTopicWithDefaultReplicationInSandbox() {
    executeStatement(
        // When:
        "CREATE STREAM createNewTopic (a INT, b INT)"
            + " WITH (KAFKA_TOPIC='foo', PARTITIONS=1, FORMAT='JSON');"

        // Then: dependent statement also executes successfully
        + "CREATE STREAM pickupDefaultReplicationFactor AS SELECT * FROM createNewTopic;"
    );
  }

  @Test
  public void shouldUseTopicFromDependentStatementInSandbox() {
    executeStatement(
        // When:
        "CREATE STREAM createNewTopic (a INT, b INT)"
            + " WITH (KAFKA_TOPIC='bar', PARTITIONS=1, FORMAT='JSON');"

        // Then: dependent statement also executes successfully
        + "CREATE STREAM reuseTopic WITH (KAFKA_TOPIC='bar') AS SELECT * FROM createNewTopic;"
    );
  }
  @Test

  public void shouldRegisterAvroSchemaInSandboxViaCS() {
    executeStatement(
      // When:
      "CREATE STREAM avro_input (a INT KEY, b INT KEY, c VARCHAR, d VARCHAR)"
        + " WITH (KAFKA_TOPIC='t2', PARTITIONS=1, FORMAT='AVRO');"

      // Then: dependent statement also executes successfully
      + "CREATE STREAM should_infer_schema WITH (KAFKA_TOPIC='t2', FORMAT='AVRO');"
    );
  }

  @Test
  public void shouldRegisterAvroSchemaInSandboxViaCSAS() {
    // Given:
    executeStatement(
      "CREATE STREAM avro_input (a INT KEY, b INT KEY, c VARCHAR, d VARCHAR)"
        + " WITH (KAFKA_TOPIC='t5', PARTITIONS=1, FORMAT='AVRO');"
    );
    executeStatement(
      // When:
      "CREATE STREAM should_register_schema WITH (KAFKA_TOPIC='t6', FORMAT='AVRO') AS"
        + " SELECT * FROM avro_input;"

      // Then: dependent statement also executes successfully
      + "CREATE STREAM should_infer_schema WITH (KAFKA_TOPIC='t6', FORMAT='AVRO');"
    );
  }

  @Test
  public void shouldRegisterAvroSchemaInSandboxViaCSandSchemaId() throws Exception {
    // Given:
    executeStatement(
      "CREATE STREAM avro_input (a INT KEY, b INT KEY, c VARCHAR, d VARCHAR)"
        + " WITH (KAFKA_TOPIC='t9', PARTITIONS=1, FORMAT='AVRO');"
    );
    final SchemaRegistryClient srClient = TEST_HARNESS.getSchemaRegistryClient();
    final int keySchemaId = srClient.getLatestSchemaMetadata("t9-key").getId();
    final int valueSchemaId = srClient.getLatestSchemaMetadata("t9-value").getId();

    executeStatement(
      // When:
      "CREATE STREAM should_register_schema WITH ("
        + "KAFKA_TOPIC='t10',"
        + "FORMAT='AVRO',"
        + "KEY_SCHEMA_ID=%s,"
        + "VALUE_SCHEMA_ID=%s"
      + ") AS "
        // because ksqlDB always assumes an unwrapped key schema, both columns `a` and `b`
        // are inferred as `struct<a,b>` (cf https://github.com/confluentinc/ksql/issues/8489);
        // thus, we need to create a `struct<a,b>` key with default name `rowkey`
        // to get a logical schema that is compatible to the physical schema
      + "SELECT Struct(a := a, b := b) AS rowkey, c, d FROM avro_input PARTITION BY Struct(a := a, b := b);"

      // Then: dependent statement also executes successfully
      + "CREATE STREAM should_infer_schema WITH (KAFKA_TOPIC='t10', FORMAT='AVRO');",
      String.valueOf(keySchemaId),
      String.valueOf(valueSchemaId)
    );
  }

  private void executeStatement(
      final String statement,
      final String... args
  ) {
    final String formatted = format(statement, (Object[])args);

    final List<QueryMetadata> queries = ksqlContext.sql(formatted);

    final List<QueryMetadata> newQueries = queries.stream()
        .filter(q -> !(q instanceof PersistentQueryMetadata))
        .collect(Collectors.toList());

    newQueries.forEach(QueryMetadata::start);

    toClose.addAll(newQueries);
  }

}
