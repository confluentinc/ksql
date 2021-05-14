/*
 * Copyright 2021 Confluent Inc.
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

import org.apache.kafka.common.acl.AclOperation;

/**
 * An interface that provides ACL validation on Kafka topics.
 */
public interface KsqlAccessValidator {
  /**
   * Checks if an authenticated user provided by the {@code securityContext} has authorization
   * to execute the {@code operation} on the kafka {@code topicName}.
   *
   * @param securityContext The context for the authenticated user.
   * @param topicName The topic name to check access.
   * @param operation The {@code AclOperation} to validate against the {@code topicName}.
   */
  void checkAccess(KsqlSecurityContext securityContext, String topicName, AclOperation operation);
}
