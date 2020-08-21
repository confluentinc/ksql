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

import static io.confluent.ksql.rest.entity.StreamedRowMatchers.matchersRows;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.TestBasicJaasConfig;
import io.confluent.ksql.util.UserDataProvider;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.security.JaasUtils;
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
 * Test to ensure pull queries route across multiple KSQL nodes correctly.
 *
 * <p>For tests on general syntax and handled see RestQueryTranslationTest's
 * materialized-aggregate-static-queries.json
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({IntegrationTest.class})
public class PullQueryFunctionalTest {

  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final String USER_TOPIC = "user_topic";
  private static final String USERS_STREAM = "users";

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_CLUSTER_ID = "ksql-42";
  private static final String USER_WITH_ACCESS = "harry";
  private static final String USER_WITH_ACCESS_PWD = "changeme";

  private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
  private static final Format VALUE_FORMAT = FormatFactory.JSON;
  private static final int HEADER = 1;

  private static final TestBasicJaasConfig JAAS_CONFIG = TestBasicJaasConfig
      .builder(PROPS_JAAS_REALM)
      .addUser(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD, KSQL_CLUSTER_ID)
      .build();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final int BASE_TIME = 1_000_000;
  private static final int ONE_SECOND = (int)TimeUnit.SECONDS.toMillis(1);

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeOptions.of()
  );

  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withBasicCredentials(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG,
          KsqlRestConfig.AUTHENTICATION_METHOD_BASIC)
      .withProperty(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
      .withProperty(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_CLUSTER_ID)
      .withProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, JAAS_CONFIG.jaasFile().toString())
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8188")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8188")
      .build();

  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withBasicCredentials(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG,
          KsqlRestConfig.AUTHENTICATION_METHOD_BASIC)
      .withProperty(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
      .withProperty(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_CLUSTER_ID)
      .withProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, JAAS_CONFIG.jaasFile().toString())
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8189")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8189")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(JAAS_CONFIG)
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

    makeAdminRequest(
        "CREATE STREAM " + USERS_STREAM
            + " (" + USER_PROVIDER.ksqlSchemaString(false) + ")"
            + " WITH ("
            + "   kafka_topic='" + USER_TOPIC + "', "
            + "   value_format='" + VALUE_FORMAT.name() + "'"
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

    makeAdminRequest(
        "CREATE TABLE " + output + " AS"
            + " SELECT " + USER_PROVIDER.key() + ", COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );

    waitForTableRows();

    final String sql = "SELECT * FROM " + output + " WHERE USERID = '" + key + "';";

    // When:

    final List<StreamedRow> rows_0 = makePullQueryRequest(REST_APP_0, sql);
    final List<StreamedRow> rows_1 = makePullQueryRequest(REST_APP_1, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_1, is(matchersRows(rows_0)));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(key, 1)));
  }

  @Test
  public void shouldGetSingleWindowedKeyFromBothNodes() {
    // Given:
    final String key = Iterables.get(USER_PROVIDER.data().keySet(), 0);

    makeAdminRequest(
        "CREATE TABLE " + output + " AS"
            + " SELECT " +  USER_PROVIDER.key() + ", COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " WINDOW TUMBLING (SIZE 1 SECOND)"
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );

    waitForTableRows();

    final String sql = "SELECT * FROM " + output
        + " WHERE USERID = '" + key + "'"
        + " AND WINDOWSTART = " + BASE_TIME + ";";

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(REST_APP_0, sql);
    final List<StreamedRow> rows_1 = makePullQueryRequest(REST_APP_1, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_1, is(matchersRows(rows_0)));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(
        key,                    // USERID
        BASE_TIME,              // WINDOWSTART
        BASE_TIME + ONE_SECOND, // WINDOWEND
        1                       // COUNT
    )));
  }

  private static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, validCreds());
  }

  private static void makeAdminRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP_0, sql, validCreds());
  }

  private void waitForTableRows() {
    TEST_HARNESS.verifyAvailableUniqueRows(
        output.toUpperCase(),
        USER_PROVIDER.data().size(),
        VALUE_FORMAT,
        AGGREGATE_SCHEMA
    );
  }

  private static Optional<BasicCredentials> validCreds() {
    return Optional.of(BasicCredentials.of(
        USER_WITH_ACCESS,
        USER_WITH_ACCESS_PWD
    ));
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}

