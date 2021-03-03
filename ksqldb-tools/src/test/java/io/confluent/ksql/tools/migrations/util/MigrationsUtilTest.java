/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.tools.migrations.util;

import static io.confluent.ksql.tools.migrations.util.MigrationsUtil.createClientOptions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.api.client.ClientOptions;
import org.junit.Test;

public class MigrationsUtilTest {

  private static final String URL = "http://localhost:8088";

  @Test
  public void shouldCreateNonTlsClientOptions() {
    // Given:
    final ClientOptions clientOptions = createClientOptions(URL, "user",
        "pass", null, "", null,
        null, "", null, false);

    // Then:
    assertThat(clientOptions.isUseTls(), is(false));
  }

  @Test
  public void shouldCreateTlsClientOptions() {
    // Given:
    final ClientOptions clientOptions = createClientOptions(URL, "user",
        "pass", "abc", null, null,
        null, null, null, true);

    // Then:
    assertThat(clientOptions.isUseTls(), is(true));
  }
}
