/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.embedded.KsqlContext;
import io.confluent.ksql.embedded.KsqlContextTestUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlContextTestUtilTest {

  private static final KsqlConfig BASE_CONFIG = new KsqlConfig(ImmutableMap.of(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:10"
  ));

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private FunctionRegistry functionRegistry;

  @SuppressWarnings("unused")
  @Test
  public void shouldBeAbleToHaveTwoInstancesWithDifferentNames() {
    // Given:
    try (KsqlContext first = KsqlContextTestUtil.create(
        BASE_CONFIG,
        srClient,
        functionRegistry
    )) {
      first.terminateQuery(new QueryId("avoid compiler warning"));

      // When:
      final KsqlContext second = KsqlContextTestUtil.create(
          BASE_CONFIG,
          srClient,
          functionRegistry
      );

      // Then: did not throw due to metrics name clash.
      second.close();
    }
  }
}