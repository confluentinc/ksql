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

package io.confluent.ksql.rest.server.context;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Wraps the REST context state that may be provided by external KSQL rest filters.
 */
public class KsqlRestContext {
  public static final String KSQL_REST_CONTEXT_PROPERTY = "ksql.rest.context.property";

  private final ImmutableMap<String, Object> restContextProperties;

  /**
   * Returns the {@code KsqlRestContext} object found on the {@link ContainerRequestContext}
   *
   * @param requestContext The {@link ContainerRequestContext} where to find the KSQL rest context
   * @return The {@code KsqlRestContext}
   */
  public static Optional<KsqlRestContext> fromRequestContext(
      final ContainerRequestContext requestContext
  ) {
    return Optional.ofNullable(
        (KsqlRestContext)requestContext.getProperty(KSQL_REST_CONTEXT_PROPERTY)
    );
  }

  public KsqlRestContext(final Map<String, Object> restContextProperties) {
    this.restContextProperties = ImmutableMap.copyOf(
        Objects.requireNonNull(restContextProperties, "restContextProperties")
    );
  }

  public Map<String, Object> getRestContextProperties() {
    return restContextProperties;
  }
}
