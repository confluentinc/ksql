/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.version.metrics;

import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndProxy;
import org.mockserver.socket.PortFactory;

import java.io.IOException;
import java.util.Properties;

import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseSupportConfig;

import static org.mockserver.model.HttpRequest.request;

public class VersionCheckerIntegrationTest {

  private static int proxyPort;
  private static ClientAndProxy clientAndProxy;

  @BeforeClass
  public static void startProxy() throws Exception {
    proxyPort = PortFactory.findFreePort();
    clientAndProxy = ClientAndProxy.startClientAndProxy(proxyPort);
  }

  @Test
  public void testMetricsAgent() throws InterruptedException, IOException {
    KsqlVersionCheckerAgent versionCheckerAgent = new KsqlVersionCheckerAgent(false);
    Properties versionCheckProps = new Properties();
    versionCheckProps.setProperty(BaseSupportConfig
        .CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    versionCheckProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_CONFIG,
        "http://localhost:" + proxyPort
    );
    versionCheckerAgent.start(KsqlModuleType.LOCAL_CLI, versionCheckProps);

    TestUtils.waitForCondition(() -> {
          try {
            clientAndProxy.verify(request().withPath("/ksql/anon").withMethod("POST"));
            return true;
          } catch (AssertionError e) {
            return false;
          }
        },
        30000, "Version not submitted"
    );
  }
}
