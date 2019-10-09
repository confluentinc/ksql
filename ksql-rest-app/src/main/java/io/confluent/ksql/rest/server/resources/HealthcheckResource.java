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
import io.confluent.ksql.rest.entity.HealthcheckResponse;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.healthcheck.HealthcheckAgent;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.services.ServerInternalKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/healthcheck")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class HealthcheckResource {
  private final HealthcheckAgent healthcheckAgent;
  private final Duration healthcheckInterval;
  private final Supplier<Long> currentTimeSupplier;
  private final ResponseCache responseCache;

  @VisibleForTesting
  HealthcheckResource(
      final HealthcheckAgent healthcheckAgent,
      final Duration healthcheckInterval,
      final Supplier<Long> currentTimeSupplier
  ) {
    this.healthcheckAgent = Objects.requireNonNull(healthcheckAgent, "healthcheckAgent");
    this.healthcheckInterval = Objects.requireNonNull(healthcheckInterval, "healthcheckInterval");
    this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier, "currentTimeSupplier");
    this.responseCache = new ResponseCache(currentTimeSupplier);
  }

  @GET
  public Response checkHealth() {
    return Response.ok(getResponse()).build();
  }

  private HealthcheckResponse getResponse() {
    if (responseCache.isEmpty() || timeSinceLastResponse().compareTo(healthcheckInterval) > 0) {
      final HealthcheckResponse response = healthcheckAgent.checkHealth();
      responseCache.cache(response);
      return response;
    } else {
      return responseCache.lastResponse();
    }
  }

  private Duration timeSinceLastResponse() {
    return Duration.ofMillis(currentTimeSupplier.get() - responseCache.lastTimestamp());
  }

  public static HealthcheckResource create(
      final KsqlResource ksqlResource,
      final ServiceContext serviceContext,
      final KsqlRestConfig restConfig
  ) {
    return new HealthcheckResource(
        new HealthcheckAgent(
            new ServerInternalKsqlClient(ksqlResource, serviceContext),
            restConfig),
        Duration.ofMillis(restConfig.getLong(KsqlRestConfig.KSQL_HEALTHCHECK_INTERVAL_MS_CONFIG)),
        System::currentTimeMillis
    );
  }

  private static class ResponseCache {
    private final Supplier<Long> currentTimeSupplier;
    private HealthcheckResponse response;
    private long timestamp;

    ResponseCache(final Supplier<Long> currentTimeSupplier) {
      this.currentTimeSupplier = Objects.requireNonNull(currentTimeSupplier, "currentTimeSupplier");
    }

    void cache(final HealthcheckResponse response) {
      this.response = response;
      this.timestamp = currentTimeSupplier.get();
    }

    boolean isEmpty() {
      return response == null;
    }

    HealthcheckResponse lastResponse() {
      return response;
    }

    long lastTimestamp() {
      return timestamp;
    }
  }
}
