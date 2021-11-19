/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.http.ServerWebSocket;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WSQueryEndpointTest {
  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  @Mock
  private ServerWebSocket serverWebSocket;
  @Mock
  private KsqlSecurityContext ksqlSecurityContext;
  @Mock
  private DenyListPropertyValidator denyListPropertyValidator;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Context context;
  @Mock
  private QueryExecutor queryExecutor;

  private WSQueryEndpoint wsQueryEndpoint;

  @Before
  public void setUp() {
    wsQueryEndpoint = new WSQueryEndpoint(
        ksqlConfig,
        mock(StatementParser.class),
        mock(KsqlEngine.class),
        mock(CommandQueue.class),
        mock(ListeningScheduledExecutorService.class),
        mock(ActivenessRegistrar.class),
        mock(Duration.class),
        Optional.empty(),
        mock(Errors.class),
        denyListPropertyValidator,
        queryExecutor
    );
  }

  @Test
  public void shouldCallPropertyValidatorOnExecuteStream()
      throws JsonProcessingException {
    // Given
    final Map<String, Object> overrides =
        ImmutableMap.of(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    final MultiMap params = buildRequestParams("show streams;", overrides);

    // When
    executeStreamQuery(params);

    // Then
    // WS sockets do not throw any exception (closes silently). We can only verify the validator
    // was called.
    verify(denyListPropertyValidator).validateAll(overrides);
  }

  private MultiMap buildRequestParams(final String command, final Map<String, Object> streamProps)
      throws JsonProcessingException {
    final MultiMap params = MultiMap.caseInsensitiveMultiMap();
    final KsqlRequest request = new KsqlRequest(
        command, streamProps, Collections.emptyMap(), 1L);

    params.add("request", OBJECT_MAPPER.writeValueAsString(request));
    return params;
  }

  private void executeStreamQuery(final MultiMap params) {
    wsQueryEndpoint.executeStreamQuery(serverWebSocket, params, ksqlSecurityContext, context);
  }
}
