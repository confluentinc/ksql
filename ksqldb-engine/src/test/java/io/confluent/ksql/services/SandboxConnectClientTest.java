/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import java.util.List;
import java.util.Map;
import io.confluent.ksql.rest.entity.ConfigInfos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SandboxConnectClientTest {

  @Mock
  private ConnectClient delegate;
  @Mock
  private ConnectResponse<ConfigInfos> mockValidateResponse;
  @Mock
  private ConnectResponse<List<String>> mockListResponse;

  private ConnectClient sandboxClient;

  @Before
  public void setUp() {
    sandboxClient = SandboxConnectClient.createProxy(delegate);
  }

  @Test
  public void shouldForwardOnValidate() {
    // Given:
    final Map<String, String> config = ImmutableMap.of("foo", "bar");
    when(delegate.validate("plugin", config)).thenReturn(mockValidateResponse);

    // When:
    final ConnectResponse<ConfigInfos> validateResponse = sandboxClient.validate("plugin", config);

    // Then:
    assertThat(validateResponse, is(mockValidateResponse));
  }

  @Test
  public void shouldForwardOnList() {
    // Given:
    when(delegate.connectors()).thenReturn(mockListResponse);

    // When:
    final ConnectResponse<List<String>> listResponse = sandboxClient.connectors();

    // Then:
    assertThat(listResponse, is(mockListResponse));
  }

}