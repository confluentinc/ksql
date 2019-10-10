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

package io.confluent.ksql.rest.integration;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.avro.AvroSchemas;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class KsqlResourceFunctionalTest {

  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";

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

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);
    RestIntegrationTestUtil.createStreams(REST_APP, PAGE_VIEW_STREAM, PAGE_VIEW_TOPIC);
  }

  @After
  public void cleanUp() {
    REST_APP.closePersistentQueries();
    REST_APP.dropSourcesExcept(PAGE_VIEW_STREAM);
  }

  @Test
  public void shouldDistributeMultipleInterDependantDmlStatements() {
    // When:
    final List<KsqlEntity> results = makeKsqlRequest(
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
            + "CREATE STREAM S2 AS SELECT * FROM S;"
    );

    // Then:
    assertThat(results, contains(
        instanceOf(CommandStatusEntity.class),
        instanceOf(CommandStatusEntity.class)
    ));

    assertSuccessful(results);

    assertThat(REST_APP.getPersistentQueries(), hasItems(
        startsWith("CSAS_S_"),
        startsWith("CSAS_S2_")
    ));
  }

  @Test
  public void shouldHandleInterDependantExecutableAndNonExecutableStatements() {
    // When:
    final List<KsqlEntity> results = makeKsqlRequest(
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
            + "DESCRIBE S;"
    );

    // Then:
    assertThat(results, contains(
        instanceOf(CommandStatusEntity.class),
        instanceOf(SourceDescriptionEntity.class)
    ));
  }

  @Test
  public void shouldHandleInterDependantCsasTerminateAndDrop() {
    // When:
    final List<KsqlEntity> results = makeKsqlRequest(
        "CREATE STREAM SS AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    final String query = REST_APP.getPersistentQueries().iterator().next();
    results.addAll(makeKsqlRequest("TERMINATE " + query + ";"
        + "DROP STREAM SS;"));

    // Then:
    assertThat(results, contains(
        instanceOf(CommandStatusEntity.class),
        instanceOf(CommandStatusEntity.class),
        instanceOf(CommandStatusEntity.class)
    ));

    assertSuccessful(results);
  }

  @Test
  public void shouldInsertIntoValuesForAvroTopic() throws Exception {
    // Given:
    final PhysicalSchema schema = PhysicalSchema.from(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("AUTHOR"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("TITLE"), SqlTypes.STRING)
            .build(),
        SerdeOption.none()
    );

    TEST_HARNESS.getSchemaRegistryClient()
        .register(
            "books" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX,
            AvroSchemas.getAvroSchema(
                schema.valueSchema(),
                "books_value",
                new KsqlConfig(REST_APP.getBaseConfig())
            )
        );

    // When:
    final List<KsqlEntity> results = makeKsqlRequest(""
        + "CREATE STREAM books (author VARCHAR, title VARCHAR) "
        + "WITH (kafka_topic='books', key='author', value_format='avro', partitions=1);"
        + " "
        + "INSERT INTO BOOKS (ROWTIME, author, title) VALUES (123, 'Metamorphosis', 'Franz Kafka');"
    );

    // Then:
    assertSuccessful(results);

    TEST_HARNESS.verifyAvailableRows(
        "books",
        contains(matches(
            "Metamorphosis",
            new GenericRow(ImmutableList.of("Metamorphosis", "Franz Kafka")),
            0,
            0L,
            123L)),
        Format.AVRO,
        schema
    );
  }

  @SuppressWarnings("SameParameterValue")
  private static Matcher<ConsumerRecord<String, GenericRow>> matches(
      final String key,
      final GenericRow value,
      final int partition,
      final long offset,
      final long timestamp
  ) {
    return new TypeSafeMatcher<ConsumerRecord<String, GenericRow>>() {
      @Override
      protected boolean matchesSafely(final ConsumerRecord<String, GenericRow> item) {
        return item.key().equalsIgnoreCase(key)
            && item.value().equals(value)
            && item.offset() == offset
            && item.partition() == partition
            && item.timestamp() == timestamp;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(
            String.format("Expected key: %s value: %s partition: %d offset: %d timestamp: %d",
                key,
                value,
                partition,
                offset,
                timestamp));
      }
    };
  }

  private static void assertSuccessful(final List<KsqlEntity> results) {
    results.stream()
        .filter(e -> e instanceof CommandStatusEntity)
        .map(CommandStatusEntity.class::cast)
        .forEach(r -> assertThat(
            r.getStatementText() + " : " + r.getCommandStatus().getMessage(),
            r.getCommandStatus().getStatus(),
            is(Status.SUCCESS)));
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }
}