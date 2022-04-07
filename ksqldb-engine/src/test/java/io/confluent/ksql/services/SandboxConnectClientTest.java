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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.test.util.OptionalMatchers;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
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

  private ConnectClient sandboxClient;

  @Before
  public void setUp() {
    sandboxClient = SandboxConnectClient.createProxy(delegate);
  }

  @Test
  public void shouldReturnStubSuccessOnCreate() {
    // When:
    final ConnectResponse<ConnectorInfo> createResponse =
        sandboxClient.create("foo", ImmutableMap.of());

    // Then:
    assertThat(createResponse.datum(), OptionalMatchers.of(is(
        new ConnectorInfo("dummy", ImmutableMap.of(), ImmutableList.of(), ConnectorType.UNKNOWN))));
    assertThat("expected no error", !createResponse.error().isPresent());
  }

  @Test
  public void shouldReturnEmptyListOnList() {
    // When:
    final ConnectResponse<List<String>> listResponse = sandboxClient.connectors();

    // Then:
    assertThat(listResponse.datum(), OptionalMatchers.of(is(ImmutableList.of())));
    assertThat("expected no error", !listResponse.error().isPresent());
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

}