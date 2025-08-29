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

package io.confluent.ksql.rest.server;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public final class TestKsqlRestAppTest {

  @ClassRule
  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @Test
  public void shouldBeAbleToHaveTwoInstancesWithDifferentNames() {
    // Given:
    final TestKsqlRestApp first = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .build();

    final TestKsqlRestApp second = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .build();

    first.start();

    try {
      // When:
      second.start();

      // Then: did not throw due to metrics name clash.
    } finally {
      first.stop();
      second.stop();
    }
  }
}