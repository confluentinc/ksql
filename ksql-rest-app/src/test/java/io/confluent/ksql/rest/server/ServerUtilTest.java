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

import java.util.Collections;
import io.confluent.rest.RestConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.test.TestUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerUtilTest {

  @Before
  public void setUp() { }

  @Test(expected = ConfigException.class)
  public void shouldThrowConfigExceptionIfInvalidServerAddress() {
    // Given:
    KsqlRestConfig restConfig =
        new KsqlRestConfig(
            Collections.singletonMap(RestConfig.LISTENERS_CONFIG,
                "invalid"));

    // Then:
    ServerUtil.getServerAddress(restConfig);
  }

  @Test
  public void shouldReturnServerAddress() {
    // Given:
    KsqlRestConfig restConfig =
        new KsqlRestConfig(
            Collections.singletonMap(RestConfig.LISTENERS_CONFIG,
                "http://localhost:8088, http://localhost:9099"));

    // Then:
    ServerUtil.getServerAddress(restConfig);
  }

}
