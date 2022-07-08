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

import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.acl.AclOperation;

/**
 * An implementation of {@link KsqlAccessValidator} that provides authorization checks
 * from the backend services.
 */
public class KsqlBackendAccessValidator implements KsqlAccessValidator {
  @Override
  public void checkTopicAccess(
      final KsqlSecurityContext securityContext,
      final String topicName,
      final AclOperation operation
  ) {
    final Set<AclOperation> authorizedOperations = securityContext.getServiceContext()
        .getTopicClient().describeTopic(topicName).authorizedOperations();

    // Kakfa 2.2 or lower do not support authorizedOperations(). In case of running on a
    // unsupported broker version, then the authorizeOperation will be null.
    if (authorizedOperations != null && !authorizedOperations.contains(operation)) {
      // This error message is similar to what Kafka throws when it cannot access the topic
      // due to an authorization error. I used this message to keep a consistent message.
      throw new KsqlTopicAuthorizationException(operation, Collections.singleton(topicName));
    }
  }

  @Override
  public void checkSubjectAccess(
      final KsqlSecurityContext securityContext,
      final String subjectName,
      final AclOperation operation
  ) {
    // Nothing to do. Checking permissions for Schema Registry require an external authorization
    // provider. Schema Registry does not have an API to list the allowed permissions for
    // the user in a specified subject.
    return;
  }
}
