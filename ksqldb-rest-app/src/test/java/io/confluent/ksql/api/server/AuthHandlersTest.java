/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class AuthHandlersTest {
  @Mock
  private RoutingContext routingContext;
  @Test
  public void shouldSelectSkipProviderForUnauthorizedPaths() {
    // Given:
    Mockito.when(routingContext.normalizedPath()).thenReturn("/unauthorized_path");
    Map<String, Object> data = mock(Map.class);
    Mockito.when(routingContext.data()).thenReturn(data);
    Pattern p = Pattern.compile("/unauthorized.*");

    // When:
    AuthHandlers.selectHandler(routingContext, p, false, true);

    // Then (make sure "skip" provider is selected):
    Mockito.verify(data).put("provider", AuthHandlers.Provider.SKIP);
    Mockito.verify(routingContext).next();
  }

  @Test
  public void shouldNotSelectSkipProviderForAuthorizedPaths() {
    // Given:
    Mockito.when(routingContext.normalizedPath()).thenReturn("/authorized_path");
    Map<String, Object> data = mock(Map.class);
    Mockito.when(routingContext.data()).thenReturn(data);
    HttpServerRequest request = mock(HttpServerRequest.class);
    Mockito.when(routingContext.request()).thenReturn(request);
    Pattern p = Pattern.compile("/unauthorized.*");

    // When:
    AuthHandlers.selectHandler(routingContext, p, false, true);

    // Then (make sure the authorization "work" is not skipped):
    Mockito.verify(data).put("provider", AuthHandlers.Provider.PLUGIN);
    Mockito.verify(routingContext).next();
  }

  @Test
  public void shouldBuildRegexThatRespectsSkipPaths() {
    // Given:
    final List<String> configs = ImmutableList.of("/heartbeat", "/lag");

    // When:
    final Pattern skips = AuthHandlers.getAuthenticationSkipPathPattern(configs);

    // Then:
    assertThat(skips.matcher("/heartbeat").matches(), is(true));
    assertThat(skips.matcher("/lag").matches(), is(true));
    assertThat(skips.matcher("/foo").matches(), is(false));
  }

}