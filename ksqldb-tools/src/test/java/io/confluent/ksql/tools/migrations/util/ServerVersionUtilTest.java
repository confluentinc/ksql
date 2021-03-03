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

import static io.confluent.ksql.tools.migrations.util.ServerVersionUtil.isSupportedVersion;
import static io.confluent.ksql.tools.migrations.util.ServerVersionUtil.versionSupportsMultiKeyPullQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class ServerVersionUtilTest {

  @Test
  public void shouldReturnSupportedVersion() {
    assertThat(isSupportedVersion("v6.0.0"), is(true));
    assertThat(isSupportedVersion("v6.1.0"), is(true));
    assertThat(isSupportedVersion("v6.2.1"), is(true));
    assertThat(isSupportedVersion("v0.10.0"), is(true));
    assertThat(isSupportedVersion("v0.10.1"), is(true));
    assertThat(isSupportedVersion("v0.11.0"), is(true));
    assertThat(isSupportedVersion("v0.14.0"), is(true));
    assertThat(isSupportedVersion("v0.14.0-rc899"), is(true));
    assertThat(isSupportedVersion("v0.14.0-rc899-ksqldb"), is(true));

    assertThat(isSupportedVersion("6.0.0"), is(true));
    assertThat(isSupportedVersion("6.1.0"), is(true));
    assertThat(isSupportedVersion("6.2.1"), is(true));
    assertThat(isSupportedVersion("0.10.0"), is(true));
    assertThat(isSupportedVersion("0.10.1"), is(true));
    assertThat(isSupportedVersion("0.11.0"), is(true));
    assertThat(isSupportedVersion("0.14.0"), is(true));
    assertThat(isSupportedVersion("0.14.0-rc899"), is(true));
    assertThat(isSupportedVersion("0.14.0-rc899-ksqldb"), is(true));
  }

  @Test
  public void shouldReturnUnsupportedVersion() {
    assertThat(isSupportedVersion("v5.5.5"), is(false));
    assertThat(isSupportedVersion("v5.4.0"), is(false));
    assertThat(isSupportedVersion("v4.0.1"), is(false));
    assertThat(isSupportedVersion("v0.9.5"), is(false));
    assertThat(isSupportedVersion("v0.8.0"), is(false));
    assertThat(isSupportedVersion("v0.6.0"), is(false));
    assertThat(isSupportedVersion("v0.6.0-rc123"), is(false));
    assertThat(isSupportedVersion("v0.6.0-ksqldb"), is(false));

    assertThat(isSupportedVersion("5.5.5"), is(false));
    assertThat(isSupportedVersion("5.4.0"), is(false));
    assertThat(isSupportedVersion("4.0.1"), is(false));
    assertThat(isSupportedVersion("0.9.5"), is(false));
    assertThat(isSupportedVersion("0.8.0"), is(false));
    assertThat(isSupportedVersion("0.6.0"), is(false));
    assertThat(isSupportedVersion("0.6.0-rc123"), is(false));
    assertThat(isSupportedVersion("0.6.0-ksqldb"), is(false));
  }

  @Test
  public void shouldReturnMultiKeyPullQueriesSupported() {
    assertThat(versionSupportsMultiKeyPullQuery("v6.1.0"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("v6.2.1"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("v0.14.0"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("v0.14.1"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("v0.15.0"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("v0.15.0-rc899"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("v0.15.0-rc899-ksqldb"), is(true));

    assertThat(versionSupportsMultiKeyPullQuery("6.1.0"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("6.2.1"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("0.14.0"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("0.14.1"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("0.15.0"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("0.15.0-rc899"), is(true));
    assertThat(versionSupportsMultiKeyPullQuery("0.15.0-rc899-ksqldb"), is(true));
  }

  @Test
  public void shouldReturnMultiKeyPullQueriesUnsupported() {
    assertThat(versionSupportsMultiKeyPullQuery("v6.0.3"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v6.0.0"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v5.5.5"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v4.0.1"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v0.13.5"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v0.8.0"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v0.6.0"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v0.6.0-rc123"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("v0.6.0-ksqldb"), is(false));

    assertThat(versionSupportsMultiKeyPullQuery("6.0.3"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("6.0.0"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("5.5.5"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("4.0.1"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("0.13.5"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("0.8.0"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("0.6.0"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("0.6.0-rc123"), is(false));
    assertThat(versionSupportsMultiKeyPullQuery("0.6.0-ksqldb"), is(false));
  }
}