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

import com.google.common.base.Ticker;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAccessValidatorCacheTest {
  private static final String TOPIC_1 = "topic1";
  private static final long ONE_SEC_IN_NS = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

  @Mock
  private KsqlSecurityContext securityContext;
  @Mock
  private KsqlAccessValidator backendValidator;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Ticker fakeTicker;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlAccessValidator cache;

  @Before
  public void setUp() {
    when(ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME)).thenReturn(1L);
    when(ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES)).thenReturn(10L);
    when(fakeTicker.read()).thenReturn(System.nanoTime());

    cache = new KsqlAccessValidatorCache(ksqlConfig, backendValidator, fakeTicker);
  }

  @Test
  public void shouldCheckBackendValidatorOnFirstRequest() {
    // When
    cache.checkAccess(securityContext, TOPIC_1, AclOperation.READ);

    // Then
    verify(backendValidator, times(1))
        .checkAccess(securityContext, TOPIC_1, AclOperation.READ);
    verifyNoMoreInteractions(backendValidator);
  }

  @Test
  public void shouldCheckCacheValidatorOnSecondRequest() {
    // When
    cache.checkAccess(securityContext, TOPIC_1, AclOperation.READ);
    when(fakeTicker.read()).thenReturn(ONE_SEC_IN_NS);
    cache.checkAccess(securityContext, TOPIC_1, AclOperation.READ);

    // Then
    verify(backendValidator, times(1))
        .checkAccess(securityContext, TOPIC_1, AclOperation.READ);
    verifyNoMoreInteractions(backendValidator);
  }

  @Test
  public void shouldThrowAuthorizationExceptionWhenBackendValidatorIsDenied() {
    // Given
    doThrow(KsqlTopicAuthorizationException.class).when(backendValidator)
        .checkAccess(securityContext, TOPIC_1, AclOperation.READ);

    // Then
    expectedException.expect(KsqlTopicAuthorizationException.class);

    // When
    cache.checkAccess(securityContext, TOPIC_1, AclOperation.READ);
  }

  @Test
  public void shouldThrowExceptionWhenBackendValidatorThrowsAnException() {
    // Given
    doThrow(RuntimeException.class).when(backendValidator)
        .checkAccess(securityContext, TOPIC_1, AclOperation.READ);

    // Then
    expectedException.expect(RuntimeException.class);

    // When
    cache.checkAccess(securityContext, TOPIC_1, AclOperation.READ);
  }
}
