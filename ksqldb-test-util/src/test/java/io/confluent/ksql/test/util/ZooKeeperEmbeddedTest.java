/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.test.util;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;


public class ZooKeeperEmbeddedTest {

  // Use the same timeouts as KafkaEmbedded so that this test highlights any config issues
  private static final int SESSION_TIMEOUT_MS = (int) EmbeddedSingleNodeKafkaCluster
      .ZK_SESSION_TIMEOUT.toMillis();

  private static final long CONNECT_TIMEOUT_MS = (int) EmbeddedSingleNodeKafkaCluster
      .ZK_CONNECT_TIMEOUT.toMillis();

  @Test
  public void shouldSupportMultipleInstancesRunning() throws Exception {
    // Given:
    final ZooKeeperEmbedded first = new ZooKeeperEmbedded();

    try {
      // When:
      final ZooKeeperEmbedded second = new ZooKeeperEmbedded();

      // Then:
      assertCanConnect(first, "first");
      assertCanConnect(second, "second");

      second.stop();
    } finally {
      first.stop();
    }
  }

  private static void assertCanConnect(
      final ZooKeeperEmbedded server,
      final String name
  ) {
    final CountDownLatch connectionLatch = new CountDownLatch(1);

    final Watcher watcher = event -> {
      if (event.getState() == KeeperState.SyncConnected) {
        connectionLatch.countDown();
      }
    };

    ZooKeeper zooKeeper = null;

    try {
      final String connectString = server.connectString();
      zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT_MS, watcher);
      final boolean success = connectionLatch.await(CONNECT_TIMEOUT_MS, TimeUnit.SECONDS);
      assertThat("Can not connect to " + name + " on " + connectString, success);

    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (zooKeeper != null) {
        try {
          zooKeeper.close(0);
        } catch (final Exception e) {
          System.err.println(e.getMessage());
          e.printStackTrace(System.err);
        }
      }
    }
  }
}