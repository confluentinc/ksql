/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.version.metrics;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.Matchers.is;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseSupportConfig;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class VersionCheckerIntegrationTest {

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(
      WireMockConfiguration.wireMockConfig()
          .dynamicPort()
  );

  @Test
  public void testMetricsAgent() throws InterruptedException {
    WireMock.stubFor(WireMock.post("/ksql/anon").willReturn(WireMock.ok()));

    final KsqlVersionCheckerAgent versionCheckerAgent = new KsqlVersionCheckerAgent(
        () -> false
    );
    final Properties versionCheckProps = new Properties();
    versionCheckProps.setProperty(BaseSupportConfig
        .CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    versionCheckProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_CONFIG,
        "http://localhost:" + wireMockRule.port()
    );
    versionCheckerAgent.start(KsqlModuleType.SERVER, versionCheckProps);

    assertThatEventually("Version not submitted", () -> {
          try {
            WireMock.verify(WireMock.postRequestedFor(WireMock.urlPathEqualTo("/ksql/anon")));
            return true;
          } catch (final AssertionError e) {
            return false;
          }
        },
        is(true),
        30000,
        TimeUnit.MILLISECONDS
    );
  }
}
