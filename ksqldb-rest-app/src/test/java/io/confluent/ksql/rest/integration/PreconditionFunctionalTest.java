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

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.KsqlServerPrecondition;
import io.confluent.ksql.rest.server.TestKsqlRestAppWaitingOnPrecondition;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class PreconditionFunctionalTest {

  private static final String SERVER_PRECONDITIONS_CONFIG = "ksql.server.preconditions";

  private static final int CUSTOM_ERROR_CODE = 50370;
  private static final CountDownLatch latch = new CountDownLatch(1);

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestAppWaitingOnPrecondition REST_APP = TestKsqlRestAppWaitingOnPrecondition
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(
          SERVER_PRECONDITIONS_CONFIG,
          "io.confluent.ksql.rest.integration.PreconditionFunctionalTest$TestFailedPrecondition")
      .buildWaitingOnPrecondition(latch);

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    REST_APP.startAndWaitForPrecondition();
  }

  @Test
  public void shouldNotServeRequestsWhileWaitingForPrecondition() {
    // When:
    final KsqlErrorMessage error =
        RestIntegrationTestUtil.makeKsqlRequestWithError(REST_APP, "SHOW STREAMS;");

    // Then:
    assertThat(error.getErrorCode(), is(CUSTOM_ERROR_CODE));
  }

  @Test
  public void shouldServeClusterHealthCheckReadyRequestsWhileWaitingForPrecondition() {
    shouldServeClusterHealthCheckRequestsWhileWaitingForPrecondition("ready");
  }

  @Test
  public void shouldServeClusterHealthCheckLiveRequestsWhileWaitingForPrecondition() {
    shouldServeClusterHealthCheckRequestsWhileWaitingForPrecondition("live");
  }

  private void shouldServeClusterHealthCheckRequestsWhileWaitingForPrecondition(String subPath) {

    // When:
    HttpResponse<Buffer> resp = RestIntegrationTestUtil.rawRestRequest(
        REST_APP, HttpVersion.HTTP_1_1, HttpMethod.GET, "/chc/" + subPath, null, Optional.empty());

    // Then:
    assertThat(resp.statusCode(), is(OK.code()));
    JsonObject jsonObject = new JsonObject(resp.body());
    assertThat(jsonObject.isEmpty(), is(true));
  }

  public static class TestFailedPrecondition implements KsqlServerPrecondition {

    private boolean first = true;

    @Override
    public Optional<KsqlErrorMessage> checkPrecondition(
      final KsqlRestConfig restConfig,
      final ServiceContext serviceContext,
      final KafkaTopicClient topicClient) {
      return fail();
    }

    private Optional<KsqlErrorMessage> fail() {
      if (first) {
        latch.countDown();
        first = false;
      }
      return Optional.of(new KsqlErrorMessage(CUSTOM_ERROR_CODE, "purposefully failed precondition"));
    }
  }
}