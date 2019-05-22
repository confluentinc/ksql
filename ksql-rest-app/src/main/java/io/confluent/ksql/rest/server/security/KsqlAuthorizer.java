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

import java.security.Principal;

/**
 * This Authorizer is used by KSQL to request access to KSQL resources.
 * </p>
 * Currently, the authorizer is only used to request access to Websocket endpoints. REST endpoints
 * should be secured by the implementation when calling {@link KsqlSecurityExtension#register()}.
 */
public interface KsqlAuthorizer {
  /**
   * Checks if a user has access to the specified {@code resourceClass} and {@code resourceMethod}.
   * </p>
   * The access requested is simply to check if the {@code user} is authorized to call the desired
   * REST/WebSocket endpoint or not. It is up to the implementation to decide the type of
   * access to verify.
   *
   * @param user The user principal requesting access authorization
   * @param resourceClass The resource class name
   * @param resourceMethod The resource method name
   * @return True if the user is permitted; False otherwise
   */
  boolean hasAccess(Principal user, Class resourceClass, String resourceMethod);
}
