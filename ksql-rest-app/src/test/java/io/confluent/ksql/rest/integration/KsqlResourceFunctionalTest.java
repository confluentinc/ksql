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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.TestKsqlRestApp;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
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
  private static final AtomicInteger NEXT_QUERY_ID = new AtomicInteger(0);

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private KsqlRestClient restClient;
  private String source;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);
    NEXT_QUERY_ID.set(0);
    createStreams(PAGE_VIEW_STREAM);
  }

  @Before
  public void setUp() {
    restClient = REST_APP.buildKsqlClient();

    source = KsqlIdentifierTestUtil.uniqueIdentifierName("source");

    createStreams(source);
  }

  @After
  public void cleanUp() {
    NEXT_QUERY_ID.addAndGet(REST_APP.getPersistentQueries().size());
    REST_APP.closePersistentQueries();
    REST_APP.dropSourcesExcept(PAGE_VIEW_STREAM);
    restClient.close();
  }

  @Test
  public void shouldDistributeMultipleInterDependantDmlStatements() {
    // When:
    final List<KsqlEntity> results = makeKsqlRequest(
        "CREATE STREAM S AS SELECT * FROM " + source + ";"
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
        "CREATE STREAM S AS SELECT * FROM " + source + ";"
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
        "CREATE STREAM SS AS SELECT * FROM " + source + ";"
            + "TERMINATE CSAS_SS_" + NEXT_QUERY_ID.get() + ";"
            + "DROP STREAM SS;"
    );

    // Then:
    assertThat(results, contains(
        instanceOf(CommandStatusEntity.class),
        instanceOf(CommandStatusEntity.class),
        instanceOf(CommandStatusEntity.class)
    ));

    assertSuccessful(results);
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

  private static List<KsqlEntity> awaitResults(final List<KsqlEntity> pending) {
    try (final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient()) {
      return pending.stream()
          .map(e -> awaitResult(e, ksqlRestClient))
          .collect(Collectors.toList());
    }
  }

  private static KsqlEntity awaitResult(
      final KsqlEntity e,
      final KsqlRestClient ksqlRestClient
  ) {
    if (!(e instanceof CommandStatusEntity)) {
      return e;
    }

    CommandStatusEntity cse = (CommandStatusEntity) e;
    final String commandId = cse.getCommandId().toString();

    while (cse.getCommandStatus().getStatus() != Status.ERROR
        && cse.getCommandStatus().getStatus() != Status.SUCCESS) {

      final RestResponse<CommandStatus> res = ksqlRestClient.makeStatusRequest(commandId);

      throwOnError(res);

      cse = new CommandStatusEntity(
          cse.getStatementText(),
          cse.getCommandId(),
          res.getResponse(),
          cse.getCommandSequenceNumber()
      );
    }

    return cse;
  }

  @SuppressWarnings("unchecked")
  private List<KsqlEntity> makeKsqlRequest(final String sql) {
    final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);

    throwOnError(res);

    return awaitResults(res.getResponse());
  }

  private static void throwOnError(final RestResponse<?> res) {
    if (res.isErroneous()) {
      throw new AssertionError("Failed to await result."
          + "msg: " + res.getErrorMessage());
    }
  }

  private static void createStreams(final String streamName) {
    final Client client = TestKsqlRestApp.buildClient();

    try (final Response response = client
        .target(REST_APP.getHttpListener())
        .path("ksql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(ksqlRequest(
            "CREATE STREAM " + streamName + " "
                + "(viewtime bigint, pageid varchar, userid varchar) "
                + "WITH (kafka_topic='" + PAGE_VIEW_TOPIC + "', value_format='json');"))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    } finally {
      client.close();
    }
  }

  private static Entity<?> ksqlRequest(final String sql) {
    return Entity.json(new KsqlRequest(sql, Collections.emptyMap(), null));
  }
}
