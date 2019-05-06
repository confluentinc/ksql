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

import static org.mockserver.model.HttpRequest.request;

import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseSupportConfig;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockserver.integration.ClientAndProxy;
import org.mockserver.socket.PortFactory;

public class VersionCheckerIntegrationTest {

  private static int proxyPort;
  private static ClientAndProxy clientAndProxy;

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @BeforeClass
  public static void startProxy() {
    proxyPort = PortFactory.findFreePort();
    clientAndProxy = ClientAndProxy.startClientAndProxy(proxyPort);
  }

  @Test
  public void testMetricsAgent() throws InterruptedException {

    final KsqlVersionCheckerAgent versionCheckerAgent = new KsqlVersionCheckerAgent(
        () -> false
    );
    final Properties versionCheckProps = new Properties();
    versionCheckProps.setProperty(BaseSupportConfig
        .CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    versionCheckProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_CONFIG,
        "http://localhost:" + proxyPort
    );
    versionCheckerAgent.start(KsqlModuleType.SERVER, versionCheckProps);

    TestUtils.waitForCondition(() -> {
          try {
            clientAndProxy.verify(request().withPath("/ksql/anon").withMethod("POST"));
            return true;
          } catch (final AssertionError e) {
            return false;
          }
        },
        30000, "Version not submitted"
    );
  }
}
