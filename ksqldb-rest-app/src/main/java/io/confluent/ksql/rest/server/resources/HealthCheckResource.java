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

package io.confluent.ksql.rest.server.resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.healthcheck.HealthCheckAgent;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.services.ServerInternalKsqlClient;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.admin.Admin;

public class HealthCheckResource {

  private static final Boolean KEY = Boolean.TRUE;

  private final LoadingCache<Boolean, HealthCheckResponse> responseCache;

  @VisibleForTesting
  HealthCheckResource(
      final HealthCheckAgent healthCheckAgent,
      final Duration healthCheckInterval
  ) {
    Objects.requireNonNull(healthCheckAgent, "healthCheckAgent");
    Objects.requireNonNull(healthCheckInterval, "healthCheckInterval");
    this.responseCache = createResponseCache(healthCheckAgent, healthCheckInterval);
  }

  public EndpointResponse checkHealth() {
    return EndpointResponse.ok(getResponse());
  }

  private HealthCheckResponse getResponse() {
    // This calls healthCheckAgent.checkHealth() if the cached result is expired
    return responseCache.getUnchecked(KEY);
  }

  public static HealthCheckResource create(
      final KsqlResource ksqlResource,
      final ServiceContext serviceContext,
      final KsqlRestConfig restConfig,
      final KsqlConfig ksqlConfig,
      final CommandRunner commandRunner,
      final Admin adminClient
  ) {
    return new HealthCheckResource(
        new HealthCheckAgent(
            new ServerInternalKsqlClient(ksqlResource,
                new KsqlSecurityContext(Optional.empty(), serviceContext)),
            restConfig,
            ksqlConfig,
            commandRunner,
            adminClient),
        Duration.ofMillis(restConfig.getLong(KsqlRestConfig.KSQL_HEALTHCHECK_INTERVAL_MS_CONFIG))
    );
  }

  private static LoadingCache<Boolean, HealthCheckResponse> createResponseCache(
      final HealthCheckAgent healthCheckAgent,
      final Duration cacheDuration
  ) {
    final CacheLoader<Boolean, HealthCheckResponse> loader =
        new CacheLoader<Boolean, HealthCheckResponse>() {
          @Override
          public HealthCheckResponse load(@Nonnull final Boolean key) {
            if (!key.equals(KEY)) {
              throw new IllegalArgumentException("Unexpected response cache key: " + key);
            }
            return healthCheckAgent.checkHealth();
          }
        };
    return CacheBuilder.newBuilder()
        .expireAfterWrite(cacheDuration.toMillis(), TimeUnit.MILLISECONDS)
        .build(loader);
  }
}
