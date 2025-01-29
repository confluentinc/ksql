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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKsqlPrincipalTest {

  private static final String KSQL_PRINCIPAL_NAME = "ksql_principal";
  private static final String OTHER_PRINCIPAL_NAME = "other_principal";
  private static final Map<String, Object> USER_PROPERTIES = ImmutableMap.of("prop", "val");

  @Mock
  private KsqlPrincipal ksqlPrincipal;
  @Mock
  private Principal otherPrincipal;

  private DefaultKsqlPrincipal wrappedKsqlPrincipal;
  private DefaultKsqlPrincipal wrappedOtherPrincipal;

  @Before
  public void setUp() {
    when(ksqlPrincipal.getName()).thenReturn(KSQL_PRINCIPAL_NAME);
    when(ksqlPrincipal.getUserProperties()).thenReturn(USER_PROPERTIES);
    when(otherPrincipal.getName()).thenReturn(OTHER_PRINCIPAL_NAME);

    wrappedKsqlPrincipal = new DefaultKsqlPrincipal(ksqlPrincipal);
    wrappedOtherPrincipal = new DefaultKsqlPrincipal(otherPrincipal);
  }

  @Test
  public void shouldReturnName() {
    assertThat(wrappedKsqlPrincipal.getName(), is(KSQL_PRINCIPAL_NAME));
    assertThat(wrappedOtherPrincipal.getName(), is(OTHER_PRINCIPAL_NAME));
  }

  @Test
  public void shouldReturnUserProperties() {
    assertThat(wrappedKsqlPrincipal.getUserProperties(), is(USER_PROPERTIES));
    assertThat(wrappedOtherPrincipal.getUserProperties(), is(Collections.emptyMap()));
  }

  @Test
  public void shouldReturnOriginalPrincipal() {
    assertThat(wrappedKsqlPrincipal.getOriginalPrincipal(), is(ksqlPrincipal));
    assertThat(wrappedOtherPrincipal.getOriginalPrincipal(), is(otherPrincipal));
  }

  @Test
  public void shouldReturnIpAndPort() {
    final KsqlPrincipal principal = wrappedKsqlPrincipal.withIpAddressAndPort("127.0.0.1", 1234);
    assertThat(principal.getPort(), is(1234));
    assertThat(principal.getIpAddress(), is("127.0.0.1"));
  }
}