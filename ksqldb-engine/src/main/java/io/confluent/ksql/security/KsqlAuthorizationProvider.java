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

import java.security.Principal;
import java.util.List;
import org.apache.kafka.common.acl.AclOperation;

/**
 * Interface that provides authorization to KSQL.
 */
public interface KsqlAuthorizationProvider {
  /**
   * Checks if the user is authorized to access the endpoint.
   *
   * @param user The user who is requesting access to the endpoint
   * @param method The endpoint method used, i.e. POST, GET, DELETE
   * @param path The endpoint path to access, i.e. "/ksql", "/ksql/terminate", "/query"*
   */
  void checkEndpointAccess(Principal user, String method, String path);

  /**
   * Checks if the user (if available) with the {@code userSecurityContext} has the specified
   * {@code privileges} on the the specified {@code objectType} and {@code objectName}.
   *
   * @param userSecurityContext The user security context which privileges will be checked
   * @param objectType The object type to check for privileges
   * @param objectName The object name to check for privileges
   * @param privileges The list of privileges to check in the resource
   */
  void checkPrivileges(KsqlSecurityContext userSecurityContext,
                       AuthObjectType objectType,
                       String objectName,
                       List<AclOperation> privileges);
}
