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
 * Wraps the REST context that may be provided by external KSQL REST filters.
 */
public class KsqlRestContext {
  private static final String KSQL_REST_CONTEXT_PROPERTY = "ksql.rest.context.property";

  /**
   * Returns the {@code KsqlRestContext} object found inside the {@link ContainerRequestContext}
   * properties.
   *
   * @param requestContext The {@code ContainerRequestContext} object.
   * @return The {@code KsqlRestContext} found.
   */
  public static Optional<KsqlRestContext> get(final ContainerRequestContext requestContext) {
    return Optional.ofNullable(
        (KsqlRestContext)requestContext.getProperty(KSQL_REST_CONTEXT_PROPERTY)
    );
  }

  /**
   * Sets the {@code KsqlRestContext} inside the {@link ContainerRequestContext} properties.
   *
   * @param requestContext The {@code ContainerRequestContext} object.
   * @param restContext The {@code KsqlRestContext} object.
   */
  public static void set(
      final ContainerRequestContext requestContext,
      final KsqlRestContext restContext
  ) {
    requestContext.setProperty(KSQL_REST_CONTEXT_PROPERTY, restContext);
  }

  private final ImmutableMap<String, Object> kafkaClientSupplierProperties;
  private final ImmutableMap<String, String> schemaRegistryClientHttpHeaders;

  public KsqlRestContext(
      final Map<String, Object> kafkaClientSupplierProperties,
      final Map<String, String> schemaRegistryHttpHeaders
  ) {
    this.kafkaClientSupplierProperties = ImmutableMap.copyOf(
        Objects.requireNonNull(kafkaClientSupplierProperties, "kafkaClientSupplierProperties")
    );

    this.schemaRegistryClientHttpHeaders = ImmutableMap.copyOf(
        Objects.requireNonNull(schemaRegistryHttpHeaders, "schemaRegistryClientHttpHeaders")
    );
  }

  public Map<String, Object> getKafkaClientSupplierProperties() {
    return kafkaClientSupplierProperties;
  }

  public Map<String, String> getSchemaRegistryClientHttpHeaders() {
    return schemaRegistryClientHttpHeaders;
  }
}
