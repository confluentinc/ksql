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

package io.confluent.ksql.security;

import io.confluent.ksql.services.ServiceContext;
import java.util.Optional;

/**
 * A class that provides KSQL security related information for KSQL user requests.
 */
public class KsqlSecurityContext {
  private final Optional<KsqlPrincipal> userPrincipal;
  private final ServiceContext serviceContext;

  public KsqlSecurityContext(
      final Optional<KsqlPrincipal> userPrincipal,
      final ServiceContext serviceContext
  ) {
    this.userPrincipal = userPrincipal;
    this.serviceContext = serviceContext;
  }

  /**
   * Returns a {@link KsqlPrincipal} object containing the name and properties of the current
   * authenticated user. If the user has not been authenticated, the method returns
   * {@code Optional.empty}.
   *
   * @return a {@link KsqlPrincipal} containing the name and properties of the user making
   *         this request; {@code Optional.empty} if the user has not been authenticated
   */
  public Optional<KsqlPrincipal> getUserPrincipal() {
    return userPrincipal;
  }

  /**
   * Returns a {@link ServiceContext} object with injected credentials of the authenticated
   * user. If KSQL does not have user authentication configured, the method returns the default
   * {@code ServiceContext} containing the KSQL server configuration (with KSQL credentials or not).
   *
   * @return a {@code ServiceContext} with injected user credentials or default KSQL server
   *         configuration.
   */
  public ServiceContext getServiceContext() {
    return serviceContext;
  }
}
