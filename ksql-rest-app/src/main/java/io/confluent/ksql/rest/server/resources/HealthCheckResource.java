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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.healthcheck.HealthCheckAgent;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.services.ServerInternalKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/healthcheck")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class HealthCheckResource {
  private final HealthCheckAgent healthCheckAgent;
  private final ResponseCache responseCache;

  @VisibleForTesting
  HealthCheckResource(
      final HealthCheckAgent healthCheckAgent,
      final Duration healthCheckInterval
  ) {
    this.healthCheckAgent = Objects.requireNonNull(healthCheckAgent, "healthCheckAgent");
    this.responseCache = new ResponseCache(healthCheckInterval);
  }

  @GET
  public Response checkHealth() {
    return Response.ok(getResponse()).build();
  }

  private HealthCheckResponse getResponse() {
    final Optional<HealthCheckResponse> response = responseCache.get();
    if (response.isPresent()) {
      return response.get();
    }

    final HealthCheckResponse fresh = healthCheckAgent.checkHealth();
    responseCache.cache(fresh);
    return fresh;
  }

  public static HealthCheckResource create(
      final KsqlResource ksqlResource,
      final ServiceContext serviceContext,
      final KsqlRestConfig restConfig
  ) {
    return new HealthCheckResource(
        new HealthCheckAgent(
            new ServerInternalKsqlClient(ksqlResource, serviceContext),
            restConfig),
        Duration.ofMillis(restConfig.getLong(KsqlRestConfig.KSQL_HEALTHCHECK_INTERVAL_MS_CONFIG))
    );
  }

  /* Caches a HealthCheckResponse for the specified duration */
  private static class ResponseCache {
    private static final Boolean KEY = true;

    private final Cache<Boolean, HealthCheckResponse> cache;

    ResponseCache(final Duration cacheDuration) {
      Objects.requireNonNull(cacheDuration, "cacheDuration");
      this.cache = CacheBuilder.newBuilder()
          .expireAfterWrite(cacheDuration.toMillis(), TimeUnit.MILLISECONDS)
          .build();
    }

    void cache(final HealthCheckResponse response) {
      cache.put(KEY, response);
    }

    Optional<HealthCheckResponse> get() {
      final HealthCheckResponse response = cache.getIfPresent(KEY);
      return response == null ? Optional.empty() : Optional.of(response);
    }
  }
}
