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

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KsqlProvidedAccessValidatorTest {
    private KsqlProvidedAccessValidator accessValidator;

    @Mock
    private KsqlSecurityContext securityContext;
    @Mock
    private KsqlAuthorizationProvider authorizationProvider;

    @Before
    public void setup() {
        accessValidator = new KsqlProvidedAccessValidator(authorizationProvider);
    }

    @Test
    public void shouldCheckTopicPrivilegesOnProvidedAccessValidator() {
        // When
        accessValidator.checkTopicAccess(securityContext, "topic1", AclOperation.WRITE);

        // Then
        verify(authorizationProvider, times(1))
            .checkPrivileges(
                securityContext,
                AuthObjectType.TOPIC,
                "topic1",
                ImmutableList.of(AclOperation.WRITE));
    }

    @Test
    public void shouldCheckSubjectTopicPrivilegesOnProvidedAccessValidator() {
        // When
        accessValidator.checkSubjectAccess(securityContext, "subject1", AclOperation.READ);

        // Then
        verify(authorizationProvider, times(1))
            .checkPrivileges(
                securityContext,
                AuthObjectType.SUBJECT,
                "subject1",
                ImmutableList.of(AclOperation.READ));
    }
}
