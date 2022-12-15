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

package io.confluent.ksql.api.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.Test;

public class AuthenticationPluginHandlerTest {

  @Test
  public void shouldBuildRegexThatRespectsSkipPaths() {
    // Given:
    final List<String> configs = ImmutableList.of("/heartbeat", "/lag");

    // When:
    final Pattern skips = AuthenticationPluginHandler.getAuthorizationSkipPaths(configs);

    // Then:
    assertThat(skips.matcher("/heartbeat").matches(), is(true));
    assertThat(skips.matcher("/lag").matches(), is(true));
    assertThat(skips.matcher("/foo").matches(), is(false));
  }

}