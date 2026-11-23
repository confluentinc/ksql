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

package io.confluent.ksql.rest.server.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {

  // Below the OS ephemeral ranges (Linux: 32768+, macOS/Windows: 49152+). Outgoing client
  // connections draw their local port from the ephemeral range, so a port we release there can be
  // grabbed by an unrelated client socket before the caller manages to bind it.
  private static final int MIN_PORT = 12000;
  private static final int MAX_PORT = 32000;
  private static final int MAX_PORT_TRIES = 100;

  // Ports already handed out by this JVM, so two calls never return the same port even if the
  // caller has not bound it yet.
  private static final Set<Integer> HANDED_OUT = ConcurrentHashMap.newKeySet();

  /**
   * Find a free port.
   *
   * <p>Note: Has a inherent race condition: after finding the free port it releases it so the
   * caller can use it. This opens up a window in which another application can grab the free
   * port, causing the test to fail.
   *
   * <p>Use only where there is no alternative: prefer listening on port 0 and reading back the
   * actual port.
   *
   * <p>To keep the window safe when tests run in parallel forks/JVMs, ports are picked at random
   * from a range below the OS ephemeral range (so outgoing connections can't take them) and
   * verified to be bindable. Ports already returned by this JVM are skipped. Falls back to an
   * OS-assigned port if the range is exhausted.
   */
  public static int findFreeLocalPort() {
    for (int i = 0; i < MAX_PORT_TRIES; i++) {
      final int candidate = ThreadLocalRandom.current().nextInt(MIN_PORT, MAX_PORT);
      if (HANDED_OUT.contains(candidate)) {
        continue;
      }
      try (ServerSocket socket = new ServerSocket(candidate)) {
        if (HANDED_OUT.add(socket.getLocalPort())) {
          return socket.getLocalPort();
        }
      } catch (IOException e) {
        // in use, try another
      }
    }
    for (int i = 0; i < MAX_PORT_TRIES; i++) {
      try (ServerSocket socket = new ServerSocket(0)) {
        if (HANDED_OUT.add(socket.getLocalPort())) {
          return socket.getLocalPort();
        }
      } catch (IOException e) {
        // try again
      }
    }
    throw new RuntimeException("Couldn't find free port");
  }
}
