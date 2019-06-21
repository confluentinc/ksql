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

package io.confluent.ksql.rest.server.security;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Supplier;
import javax.ws.rs.core.Configurable;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * This interface provides a security extension (or plugin) to the KSQL server in order to
 * protect the access to REST, Websockets, and other internal KSQL resources.
 * </p>
 * The type of security details provided is left to the implementation class. This class is
 * called only at specific registration points to let the implementation know when to register
 * a specific security plugin.
 */
public interface KsqlSecurityExtension extends AutoCloseable {
  /**
   * Initializes any implementation-specific resources for the security extension.
   *
   * @param ksqlConfig The KSQL configuration object
   */
  void initialize(KsqlConfig ksqlConfig);

  /**
   * Returns the authorization provider used to verify access permissions on KSQL resources.
   * </p>
   * If no authorization is required/enabled for KSQL, then an {@code Optional.empty()} object must
   * be returned.
   *
   * @return The (Optional) provider used for authorization requests.
   */
  Optional<KsqlAuthorizationProvider> getAuthorizationProvider();

  /**
   * Registers other security extension filters.
   * </p>
   * A {@link Configurable} is passed so that the extension can register other REST filters to
   * to the KSQL REST endpoints (i.e. Impersonation context).
   *
   * @param configurable The {@link Configurable} object where to register the security plugins.
   * @throws KsqlException If an error occurs while registering the REST security plugin.
   */
  void register(Configurable<?> configurable);

  /**
   * Constructs a {@link org.apache.kafka.streams.KafkaClientSupplier} with the user's credentials.
   *
   * @param principal The {@link Principal} whose credentials will be used.
   * @throws KsqlException If an error occurs while creating the
   * {@link org.apache.kafka.streams.KafkaClientSupplier}.
   */
  KafkaClientSupplier getKafkaClientSupplier(Principal principal) throws KsqlException;

  /**
   * Constructs a {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} supplier
   * with the user's credentials.
   *
   * @param principal The {@link Principal} whose credentials will be used.
   * @throws KsqlException If an error occurs while creating the
   * {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} supplier.
   */
  Supplier<SchemaRegistryClient> getSchemaRegistryClientSupplier(Principal principal)
      throws KsqlException;

  /**
   * Closes the current security extension. This is called in case the implementation requires
   * to clean any security data in memory, files, and/or close connections to external security
   * services.
   *
   * @throws KsqlException If an error occurs while closing the security extension.
   */
  @Override
  void close();
}
