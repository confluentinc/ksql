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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.UserDataProvider;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

/**
 * Test to ensure static queries route across multiple KSQL nodes correctly.
 *
 * <p>For tests on general syntax and handled see RestQueryTranslationTest's
 * materialized-aggregate-static-queries.json
 */
@Category({IntegrationTest.class})
public class StaticQueryFunctionalTest {

  private static final String USER_TOPIC = "user_topic";
  private static final String USERS_STREAM = "users";

  private static final TestDataProvider USER_PROVIDER = new UserDataProvider();
  private static final Format VALUE_FORMAT = Format.JSON;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final int BASE_TIME = 1_000_000;

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeOption.none()
  );

  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1);

  private String output;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(2, USER_TOPIC);

    final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);

    TEST_HARNESS.produceRows(
        USER_TOPIC,
        USER_PROVIDER,
        VALUE_FORMAT,
        timestampSupplier::getAndIncrement
    );

    makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM " + USERS_STREAM
            + " " + USER_PROVIDER.ksqlSchemaString()
            + " WITH ("
            + "   kafka_topic='" + USER_TOPIC + "', "
            + "   key='" + USER_PROVIDER.key() + "', "
            + "   value_format='" + VALUE_FORMAT + "'"
            + ");"
    );
  }

  @Before
  public void setUp() {
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();
  }

  @After
  public void cleanUp() {
    REST_APP_0.closePersistentQueries();
    REST_APP_0.dropSourcesExcept(USERS_STREAM);
  }

  @Test
  public void shouldGetSingleKeyFromBothNodes() {
    // Given:
    final String key = Iterables.get(USER_PROVIDER.data().keySet(), 0);

    makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );

    waitForTableRows();

    final String sql = "SELECT * FROM " + output + " WHERE ROWKEY = '" + key + "';";

    // When:

    final List<List<?>> rows_0 = makeStaticQueryRequest(REST_APP_0, sql);
    final List<List<?>> rows_1 = makeStaticQueryRequest(REST_APP_1, sql);

    // Then:
    assertThat(rows_0, hasSize(1));
    assertThat(rows_0.get(0), is(ImmutableList.of(key, 1)));
    assertThat(rows_1, is(rows_0));
  }

  @Test
  public void shouldGetSingleWindowedKeyFromBothNodes() {
    // Given:
    final String key = Iterables.get(USER_PROVIDER.data().keySet(), 0);

    makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " WINDOW TUMBLING (SIZE 1 SECOND)"
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );

    waitForTableRows();

    final String sql = "SELECT * FROM " + output
        + " WHERE ROWKEY = '" + key + "'"
        + " AND WINDOWSTART = " + BASE_TIME + ";";

    // When:
    final List<List<?>> rows_0 = makeStaticQueryRequest(REST_APP_0, sql);
    final List<List<?>> rows_1 = makeStaticQueryRequest(REST_APP_1, sql);

    // Then:
    assertThat(rows_0, hasSize(1));
    assertThat(rows_0.get(0), is(ImmutableList.of(key, BASE_TIME, 1)));
    assertThat(rows_1, is(rows_0));
  }

  private static List<List<?>> makeStaticQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    final List<KsqlEntity> entities = makeKsqlRequest(target, sql);
    assertThat(entities, hasSize(1));

    final KsqlEntity entity = entities.get(0);
    assertThat(entity, instanceOf(TableRowsEntity.class));
    return ((TableRowsEntity)entity).getRows();
  }

  private static List<KsqlEntity> makeKsqlRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return RestIntegrationTestUtil.makeKsqlRequest(target, sql);
  }

  private void waitForTableRows() {
    TEST_HARNESS.verifyAvailableUniqueRows(
        output.toUpperCase(),
        USER_PROVIDER.data().size(),
        VALUE_FORMAT,
        AGGREGATE_SCHEMA
    );
  }

  private static String getNewStateDir()  {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}

