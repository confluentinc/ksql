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

package io.confluent.ksql.test.util;

import java.io.IOException;
import java.net.BindException;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an in-memory, "embedded" instance of a ZooKeeper server.
 *
 * <p>The ZooKeeper server instance is automatically started when you create a new instance of this
 * class.
 */
class ZooKeeperEmbedded {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperEmbedded.class);

  private final TestingServer server;

  /**
   * Creates and starts a ZooKeeper instance.
   */
  ZooKeeperEmbedded() throws Exception {
    log.debug("Starting embedded ZooKeeper server...");
    this.server = createTestingServer();
    log.debug("Embedded ZooKeeper server at {} uses the temp directory at {}",
        server.getConnectString(), server.getTempDirectory());
  }

  void stop() throws IOException {
    log.debug("Shutting down embedded ZooKeeper server at {} ...", server.getConnectString());
    server.close();
    log.debug("Shutdown of embedded ZooKeeper server at {} completed", server.getConnectString());
  }

  /**
   * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   *
   * <p>You can use this to e.g. tell Kafka brokers how to connect to this instance.
   */
  String connectString() {
    return server.getConnectString();
  }

  private static TestingServer createTestingServer() throws Exception {
    while (true) {
      try {
        return new TestingServer();
      } catch (final BindException e) {
        /*
        There is a race condition in TestingServer between the point it picks a random port and
        the time it binds to the random port.  This occasionally results in a BindException.
        When this happens... retry!
         */
        log.info("Failed to create test ZK instance due to known race condition. Will retry", e);
      }
    }
  }
}