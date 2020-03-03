/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api;

import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.Server;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseServerTest {

  private static final Logger log = LoggerFactory.getLogger(BaseServerTest.class);

  protected Vertx vertx;
  protected WebClient client;
  protected Server server;
  protected TestEndpoints testEndpoints;

  @Before
  public void setUp() throws Exception {

    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    testEndpoints = new TestEndpoints();
    ApiServerConfig serverConfig = createServerConfig();
    createServer(serverConfig);
    this.client = createClient();
  }

  @After
  public void tearDown() {
    stopClient();
    stopServer();
    if (vertx != null) {
      vertx.close();
    }
  }

  protected void stopServer() {
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        log.error("Failed to shutdown server", e);
      }
    }
  }

  protected void stopClient() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        log.error("Failed to close client", e);
      }
    }
  }

  protected void createServer(ApiServerConfig serverConfig) {
    server = new Server(vertx, serverConfig, testEndpoints, false);
    server.start();
  }

  protected ApiServerConfig createServerConfig() {
    final Map<String, Object> config = new HashMap<>();
    config.put(ApiServerConfig.LISTENERS, "http://localhost:0");
    config.put(ApiServerConfig.VERTICLE_INSTANCES, 4);
    return new ApiServerConfig(config);
  }

  protected WebClientOptions createClientOptions() {
    return new WebClientOptions()
        .setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
        .setDefaultHost("localhost")
        .setDefaultPort(server.getListeners().get(0).getPort())
        .setReusePort(true);
  }

  protected WebClient createClient() {
    return WebClient.create(vertx, createClientOptions());
  }


}
