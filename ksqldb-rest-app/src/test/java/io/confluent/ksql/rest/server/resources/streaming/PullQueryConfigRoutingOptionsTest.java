/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.Map;
import org.junit.Test;

public class PullQueryConfigRoutingOptionsTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());
  private static final Map<String, Object> DEBUG_REQUEST =
      ImmutableMap.of(KsqlRequestConfig.KSQL_DEBUG_REQUEST, true);

  @Test
  public void shouldIgnoreDebugRequestFromPublicListener() {
    // Given:
    final PullQueryConfigRoutingOptions options = new PullQueryConfigRoutingOptions(
        KSQL_CONFIG, ImmutableMap.of(), DEBUG_REQUEST, false);

    // Then:
    assertThat(options.getIsDebugRequest(), is(false));
  }

  @Test
  public void shouldHonorDebugRequestFromInternalListener() {
    // Given:
    final PullQueryConfigRoutingOptions options = new PullQueryConfigRoutingOptions(
        KSQL_CONFIG, ImmutableMap.of(), DEBUG_REQUEST, true);

    // Then:
    assertThat(options.getIsDebugRequest(), is(true));
  }

  @Test
  public void shouldDefaultToNoDebugRequestFromInternalListener() {
    // Given:
    final PullQueryConfigRoutingOptions options = new PullQueryConfigRoutingOptions(
        KSQL_CONFIG, ImmutableMap.of(), ImmutableMap.of(), true);

    // Then:
    assertThat(options.getIsDebugRequest(), is(false));
  }
}
