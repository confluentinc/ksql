/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.server.computation.KafkaConfigStore;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
// shouldFailOnAvroWithoutSchemasIfSchemaNotEvolvable fails if run after shouldHandleJsonWithSchemas
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StandaloneExecutorFunctionalTest {

  @ClassRule
  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  private static final String AVRO_TOPIC = "avro-topic";
  private static final String JSON_TOPIC = "json-topic";
  private static int DATA_SIZE;
  private static int UNIQUE_DATA_SIZE;
  private static PhysicalSchema DATA_SCHEMA;

  @Mock
  private VersionCheckerAgent versionChecker;
  private Path queryFile;
  private StandaloneExecutor standalone;
  private String s1;
  private String s2;
  private String t1;

  @BeforeClass
  public static void classSetUp() {
    final OrderDataProvider provider = new OrderDataProvider();
    TEST_HARNESS.produceRows(AVRO_TOPIC, provider, AVRO);
    TEST_HARNESS.produceRows(JSON_TOPIC, provider, JSON);
    DATA_SIZE = provider.data().size();
    UNIQUE_DATA_SIZE = provider.finalData().size();
    DATA_SCHEMA = provider.schema();
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    queryFile = TMP.newFile().toPath();

    final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
        .putAll(KsqlConfigTestUtil.baseTestConfig())
        .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TEST_HARNESS.kafkaBootstrapServers())
        .put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
        .build();

    final Function<KsqlConfig, ServiceContext> serviceContextFactory = config ->
        TestServiceContext.create(
            new KsqlConfig(properties),
            TEST_HARNESS.getServiceContext().getSchemaRegistryClientFactory()
        );

    standalone = StandaloneExecutorFactory.create(
        (Map) properties,
        queryFile.toString(),
        ".",
        serviceContextFactory,
        KafkaConfigStore::new,
        activeQuerySupplier -> versionChecker,
        StandaloneExecutor::new
    );

    s1 = KsqlIdentifierTestUtil.uniqueIdentifierName("S1");
    s2 = KsqlIdentifierTestUtil.uniqueIdentifierName("S2");
    t1 = KsqlIdentifierTestUtil.uniqueIdentifierName("T1");
  }

  @After
  public void tearDown() throws Exception {
    standalone.shutdown();
  }

  @Test
  public void shouldHandleJsonWithSchemas() {
    // Given:
    givenScript(""
        + "CREATE STREAM S (ROWKEY STRING KEY, ORDERTIME BIGINT)"
        + "    WITH (kafka_topic='" + JSON_TOPIC + "', value_format='json');\n"
        + "\n"
        + "CREATE TABLE T (ROWKEY STRING PRIMARY KEY, ORDERTIME BIGINT) "
        + "    WITH (kafka_topic='" + JSON_TOPIC + "', value_format='json');\n"
        + "\n"
        + "SET 'auto.offset.reset' = 'earliest';"
        + "\n"
        + "CREATE STREAM " + s1 + " AS SELECT * FROM S;\n"
        + "\n"
        + "INSERT INTO " + s1 + " SELECT * FROM S;\n"
        + "\n"
        + "CREATE TABLE " + t1 + " AS SELECT * FROM T;\n"
        + "\n"
        + "UNSET 'auto.offset.reset';"
        + "\n"
        + "CREATE STREAM " + s2 + " AS SELECT * FROM S;\n");

    final PhysicalSchema dataSchema = PhysicalSchema.from(
        LogicalSchema.builder()
            .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
            .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
            .build(),
        SerdeOptions.of()
    );

    // When:
    standalone.startAsync();

    // Then:
    // CSAS and INSERT INTO both input into S1:
    TEST_HARNESS.verifyAvailableRows(s1, DATA_SIZE * 2, JSON, dataSchema);
    // CTAS only into T1:
    TEST_HARNESS.verifyAvailableUniqueRows(t1, UNIQUE_DATA_SIZE, JSON, dataSchema);
    // S2 should be empty as 'auto.offset.reset' unset:
    TEST_HARNESS.verifyAvailableUniqueRows(s2, 0, JSON, dataSchema);
  }

  @Test
  public void shouldHandleAvroWithSchemas() {
    // Given:
    givenScript(""
        + "CREATE STREAM S (ROWKEY STRING KEY, ORDERTIME BIGINT)"
        + "    WITH (kafka_topic='" + AVRO_TOPIC + "', value_format='avro');\n"
        + "\n"
        + "CREATE TABLE T (ROWKEY STRING PRIMARY KEY, ORDERTIME BIGINT) "
        + "    WITH (kafka_topic='" + AVRO_TOPIC + "', value_format='avro');\n"
        + "\n"
        + "SET 'auto.offset.reset' = 'earliest';"
        + "\n"
        + "CREATE STREAM " + s1 + " AS SELECT * FROM S;\n"
        + "\n"
        + "INSERT INTO " + s1 + " SELECT * FROM S;\n"
        + "\n"
        + "CREATE TABLE " + t1 + " AS SELECT * FROM T;\n"
        + "\n"
        + "UNSET 'auto.offset.reset';"
        + "\n"
        + "CREATE STREAM " + s2 + " AS SELECT * FROM S;\n");

    final PhysicalSchema dataSchema = PhysicalSchema.from(
        LogicalSchema.builder()
            .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
            .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
            .build(),
        SerdeOptions.of()
    );

    // When:
    standalone.startAsync();

    // Then:
    // CSAS and INSERT INTO both input into S1:
    TEST_HARNESS.verifyAvailableRows(s1, DATA_SIZE * 2, AVRO, dataSchema);
    // CTAS only into T1:
    TEST_HARNESS.verifyAvailableUniqueRows(t1, UNIQUE_DATA_SIZE, AVRO, dataSchema);
    // S2 should be empty as 'auto.offset.reset' unset:
    TEST_HARNESS.verifyAvailableUniqueRows(s2, 0, AVRO, dataSchema);
  }

  @Test
  public void shouldInferAvroSchema() {
    // Given:
    givenScript(""
        + "SET 'auto.offset.reset' = 'earliest';"
        + ""
        + "CREATE STREAM S (ROWKEY STRING KEY) WITH (kafka_topic='" + AVRO_TOPIC + "', value_format='avro');\n"
        + ""
        + "CREATE STREAM " + s1 + " AS SELECT * FROM S;");

    // When:
    standalone.startAsync();

    // Then:
    TEST_HARNESS.verifyAvailableRows(s1, DATA_SIZE, AVRO, DATA_SCHEMA);
  }

  @Test
  public void shouldFailOnAvroWithoutSchemasIfSchemaNotAvailable() {
    // Given:
    TEST_HARNESS.ensureTopics("topic-without-schema");

    givenScript(""
        + "SET 'auto.offset.reset' = 'earliest';"
        + ""
        + "CREATE STREAM S WITH (kafka_topic='topic-without-schema', value_format='avro');");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> standalone.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Schema for message values on topic topic-without-schema does not exist in the Schema Registry"));
  }

  @Test
  public void shouldFailOnAvroWithoutSchemasIfSchemaNotEvolvable() {
    // Given:
    givenIncompatibleSchemaExists(s1);

    givenScript(""
        + "SET 'auto.offset.reset' = 'earliest';"
        + ""
        + "CREATE STREAM S WITH (kafka_topic='" + AVRO_TOPIC + "', value_format='avro');\n"
        + ""
        + "CREATE STREAM " + s1 + " AS SELECT * FROM S;");

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> standalone.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("schema is incompatible with the current schema version registered for the topic"));
  }

  @Test
  public void shouldHandleComments() {
    // Given:
    givenScript(""
        + "-- Single line comment\n"
        + ""
        + "/*\n"
        + "Multi-line comment\n"
        + " */\n"
        + ""
        + "SET 'auto.offset.reset' = 'earliest';"
        + ""
        + "CREATE STREAM S /*inline comment*/ (ROWKEY STRING KEY, ID int)"
        + "    with (kafka_topic='" + JSON_TOPIC + "',value_format='json');\n"
        + "\n"
        + "CREATE STREAM " + s1 + "  AS SELECT * FROM S;");

    // When:
    standalone.startAsync();

    // Then:
    TEST_HARNESS.verifyAvailableRows(s1, DATA_SIZE, JSON, DATA_SCHEMA);
  }

  private static void givenIncompatibleSchemaExists(final String topicName) {
    final LogicalSchema logical = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("ITEMID"), SqlTypes.struct()
            .field("fred", SqlTypes.INTEGER)
            .build())
        .valueColumn(ColumnName.of("Other"), SqlTypes.BIGINT)
        .build();

    final PhysicalSchema incompatiblePhysical = PhysicalSchema.from(
        logical,
        SerdeOptions.of()
    );

    TEST_HARNESS.ensureSchema(topicName, incompatiblePhysical);
  }

  private void givenScript(final String contents) {
    try {
      Files.write(queryFile, contents.getBytes(StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new AssertionError("Failed to save query file", e);
    }
  }
}
