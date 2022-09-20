/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ShowQueriesMultiNodeWithTlsFunctionalTest {

  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
  private static final int INT_PORT00 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT01 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT10 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT11 = TestUtils.findFreeLocalPort();
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG,
          "http://localhost:" + INT_PORT00 + ",https://localhost:" + INT_PORT01)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "https://localhost:" + INT_PORT01)
      .withProperties(SERVER_KEY_STORE.keyStoreProps())
      .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG,
          SERVER_KEY_STORE.getKeyAlias())
      .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG,
          SERVER_KEY_STORE.getKeyAlias())
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG,
          "http://localhost:" + INT_PORT10 + ",https://localhost:" + INT_PORT11)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "https://localhost:" + INT_PORT11)
      .withProperties(SERVER_KEY_STORE.keyStoreProps())
      .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG,
          SERVER_KEY_STORE.getKeyAlias())
      .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG,
          SERVER_KEY_STORE.getKeyAlias())
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
  }

  @Test
  public void shouldShowAllQueries() {
    // When:
    final Supplier<String> app0Response = () -> getShowQueriesResult(REST_APP_0);
    final Supplier<String> app1Response = () -> getShowQueriesResult(REST_APP_1);

    // Then:
    assertThatEventually("App0", app0Response, is("RUNNING:2"), 60, TimeUnit.SECONDS);
    assertThatEventually("App1", app1Response, is("RUNNING:2"), 60, TimeUnit.SECONDS);
  }

  private static String getShowQueriesResult(final TestKsqlRestApp restApp) {
    final List<KsqlEntity> results = RestIntegrationTestUtil.makeKsqlRequest(
        restApp,
        "Show Queries;"
    );

    if (results.size() != 1) {
      return "Expected 1 response, got " + results.size();
    }

    final KsqlEntity result = results.get(0);

    if (!(result instanceof Queries)) {
      return "Expected Queries, got " + result;
    }

    final List<RunningQuery> runningQueries = ((Queries) result)
        .getQueries();

    if (runningQueries.size() != 1) {
      return "Expected 1 running query, got " + runningQueries.size();
    }

    return runningQueries.get(0).getStatusCount().toString();
  }

}