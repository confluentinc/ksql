/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import javax.ws.rs.core.Configurable;

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
   * Initializes the security extension. This is called in case the implementation requires
   * to initializes internal objects prior to registering necessary endpoints.
   *
   * @param config The KSQL configuration containing security required configs.
   * @throws KsqlException If an error occurs while initializing the security extension.
   */
  void initialize(KsqlConfig config) throws KsqlException;

  /**
   * Registers the security extension for the KSQL REST endpoints.
   *
   * @param configurable The {@link Configurable} object where to register the security plugins.
   * @throws KsqlException If an error occurs while registering the REST security plugin.
   */
  void registerRestEndpoints(Configurable<?> configurable) throws KsqlException;

  /**
   * Closes the current security extension. This is called in case the implementation requires
   * to clean any security data in memory, files, and/or close connections to external security
   * services.
   *
   * @throws KsqlException If an error occurs while closing the security extension.
   */
  @Override
  void close() throws KsqlException;
}
