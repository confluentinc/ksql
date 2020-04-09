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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.KsqlServerPrecondition;
import io.confluent.ksql.rest.server.TestKsqlRestAppWaitingOnPrecondition;
import io.confluent.ksql.services.ServiceContext;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class PreconditionFunctionalTest {

  private static final String SERVER_PRECONDITIONS_CONFIG = "ksql.server.preconditions";

  private static CountDownLatch latch = new CountDownLatch(1);

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
    System.out.println("starting class setup");
    REST_APP.startAndWaitForPrecondition();
    System.out.println("finished class setup");
  }

  @After
  public void cleanUp() {
    REST_APP.closePersistentQueries();
  }

  @Test
  public void shouldDistributeMultipleInterDependantDmlStatements() {
    // When:
    final List<KsqlEntity> results = makeKsqlRequest();

    // Then:
    assertThat(results, contains(
        instanceOf(CommandStatusEntity.class),
        instanceOf(CommandStatusEntity.class)
    ));

    assertSuccessful(results);
  }

  private static void assertSuccessful(final List<KsqlEntity> results) {
    results.stream()
        .filter(e -> e instanceof StreamsList)
        .map(CommandStatusEntity.class::cast)
        .forEach(r -> assertThat(
            r.getStatementText() + " : " + r.getCommandStatus().getMessage(),
            r.getCommandStatus().getStatus(),
            is(Status.SUCCESS)));
  }

  private static List<KsqlEntity> makeKsqlRequest() {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW STREAMS;");
  }

  public static class TestFailedPrecondition implements KsqlServerPrecondition {

    private boolean first = true;

    @Override
    public Optional<KsqlErrorMessage> checkPrecondition(
        final KsqlRestConfig restConfig,
        final ServiceContext serviceContext) {
      return fail();
    }

    private Optional<KsqlErrorMessage> pass() {
      if (first) {
        latch.countDown();
        first = false;
      }
      return Optional.empty();
    }

    private Optional<KsqlErrorMessage> fail() {
      if (first) {
        latch.countDown();
        first = false;
      }
      return Optional.of(new KsqlErrorMessage(40370, "purposefully failed precondition"));
    }
  }
}