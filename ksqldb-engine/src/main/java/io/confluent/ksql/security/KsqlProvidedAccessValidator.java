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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.acl.AclOperation;

/**
 * An implementation of {@link KsqlAccessValidator} that provides authorization checks
 * from a external authorization provider.
 */
public class KsqlProvidedAccessValidator implements KsqlAccessValidator {
  private final KsqlAuthorizationProvider authorizationProvider;

  public KsqlProvidedAccessValidator(final KsqlAuthorizationProvider authorizationProvider) {
    this.authorizationProvider = requireNonNull(authorizationProvider, "authorizationProvider");
  }

  @Override
  public void checkTopicAccess(
      final KsqlSecurityContext securityContext,
      final String topicName,
      final AclOperation operation
  ) {
    authorizationProvider.checkPrivileges(
        securityContext,
        AuthObjectType.TOPIC,
        topicName,
        ImmutableList.of(operation)
    );
  }

  @Override
  public void checkSubjectAccess(
      final KsqlSecurityContext securityContext,
      final String subjectName,
      final AclOperation operation
  ) {
    authorizationProvider.checkPrivileges(
        securityContext,
        AuthObjectType.SUBJECT,
        subjectName,
        ImmutableList.of(operation)
    );
  }
}
