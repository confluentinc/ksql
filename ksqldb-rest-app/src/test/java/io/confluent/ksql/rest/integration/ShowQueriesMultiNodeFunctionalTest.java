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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import com.google.errorprone.annotations.Immutable;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.security.BasicCredentials;
import io.confluent.ksql.security.Credentials;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.TestBasicJaasConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ShowQueriesMultiNodeFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
  private static final int INT_PORT0 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT1 = TestUtils.findFreeLocalPort();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", INT_PORT0);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", INT_PORT1);

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_CLUSTER_ID = "ksql-42";
  private static final String USER_WITH_ACCESS = "harry";
  private static final String USER_WITH_ACCESS_PWD = "changeme";

  private static final TestBasicJaasConfig JAAS_CONFIG = TestBasicJaasConfig
      .builder(PROPS_JAAS_REALM)
      .addUser(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD, KSQL_CLUSTER_ID)
      .build();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:" + INT_PORT0)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT0)
      .withProperty(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG, KsqlRestConfig.AUTHENTICATION_METHOD_BASIC)
      .withProperty(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
      .withProperty(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_CLUSTER_ID)
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:" + INT_PORT1)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT1)
      .withProperty(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG, KsqlRestConfig.AUTHENTICATION_METHOD_BASIC)
      .withProperty(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
      .withProperty(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_CLUSTER_ID)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(JAAS_CONFIG)
      .around(REST_APP_0)
      .around(REST_APP_1);

  @BeforeClass
  public static void setUpClass() throws InterruptedException {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER, validCreds());
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";",
        validCreds()
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
        "Show Queries;",
        validCreds()
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

  @Test
  public void shouldShowAllQueriesExtended() {
    // When:
    final Supplier<Set<KsqlHostInfoEntity>> app0HostResponse = () -> getShowQueriesExtendedResult(REST_APP_0).getKsqlHosts();
    final Supplier<Set<KsqlHostInfoEntity>> app1HostResponse = () -> getShowQueriesExtendedResult(REST_APP_1).getKsqlHosts();
    final Supplier<Set<StreamsTaskMetadata>> app0TaskMetadata = () -> getShowQueriesExtendedResult(REST_APP_0).getTasksMetadata();
    final Supplier<Set<StreamsTaskMetadata>> app1TaskMetadata = () -> getShowQueriesExtendedResult(REST_APP_1).getTasksMetadata();

    // Then:
    assertThatEventually("App0HostResponse", app0HostResponse, containsInAnyOrder(host0, host1));
    assertThatEventually("App1HostResponse", app1HostResponse, containsInAnyOrder(host0, host1));

    assertThatEventually("App0TaskMetadata", () -> app0TaskMetadata.get().size(), is(2));
    assertThatEventually("App1TaskMetadata", () -> app1TaskMetadata.get().size(), is(2));

    assertThatEventually(
        "App0TaskMetadata",
        () -> app0TaskMetadata.get().stream().map(StreamsTaskMetadata::getTaskId).collect(Collectors.toList()),
        containsInAnyOrder("0_0", "0_1"));
    assertThatEventually("App1TaskMetadata",
        () -> app1TaskMetadata.get().stream().map(StreamsTaskMetadata::getTaskId).collect(Collectors.toList()),
        containsInAnyOrder("0_0", "0_1"));
  }

  private static QueryExtendedResults getShowQueriesExtendedResult(final TestKsqlRestApp restApp) {
    final List<KsqlEntity> results = RestIntegrationTestUtil.makeKsqlRequest(
        restApp,
        "Show Queries Extended;",
        validCreds()
    );

    if (results.size() != 1) {
      return new QueryExtendedResults();
    }

    final KsqlEntity result = results.get(0);

    if (!(result instanceof QueryDescriptionList)) {
      return new QueryExtendedResults();
    }

    final List<QueryDescription> queryDescriptions = ((QueryDescriptionList) result)
        .getQueryDescriptions();

    if (queryDescriptions.size() != 1) {
      return new QueryExtendedResults();
    }

    return new QueryExtendedResults(
        queryDescriptions.get(0).getKsqlHostQueryStatus().keySet(),
        queryDescriptions.get(0).getTasksMetadata()
    );
  }

  @Immutable
  private static final class QueryExtendedResults {

    private final Set<KsqlHostInfoEntity> ksqlHosts;
    private final Set<StreamsTaskMetadata> tasksMetadata;

    private QueryExtendedResults() {
      this(Collections.emptySet(), Collections.emptySet());
    }
    private QueryExtendedResults(
        final Set<KsqlHostInfoEntity> ksqlHosts,
        final Set<StreamsTaskMetadata> tasksMetadata
    ) {
      this.ksqlHosts = Objects.requireNonNull(ksqlHosts, "ksqlHosts");
      this.tasksMetadata = Objects.requireNonNull(tasksMetadata, "tasksMetadata");
    }

    Set<KsqlHostInfoEntity> getKsqlHosts() {
      return ksqlHosts;
    }

    Set<StreamsTaskMetadata> getTasksMetadata() {
      return tasksMetadata;
    }
  }

  private static Optional<Credentials> validCreds() {
    return Optional.of(BasicCredentials.of(
        USER_WITH_ACCESS,
        USER_WITH_ACCESS_PWD
    ));
  }
}