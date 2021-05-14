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

package io.confluent.ksql.util;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * Represent the status of a ksql host in the cluster as determined by the Heartbeat agent.
 * A host can alive or dead annotated with the timestamp of the last update in status.
 */
@Immutable
public class HostStatus {

  private final boolean hostAlive;
  private final long lastStatusUpdateMs;

  public HostStatus(
      final boolean hostAlive,
      final long lastStatusUpdateMs
  ) {
    this.hostAlive = hostAlive;
    this.lastStatusUpdateMs = lastStatusUpdateMs;
  }

  public HostStatus withHostAlive(final boolean hostAlive) {
    return new HostStatus(hostAlive, lastStatusUpdateMs);
  }

  public HostStatus withLastStatusUpdateMs(final long lastStatusUpdateMs) {
    return new HostStatus(hostAlive, lastStatusUpdateMs);
  }

  public long getLastStatusUpdateMs() {
    return lastStatusUpdateMs;
  }

  public boolean isHostAlive() {
    return hostAlive;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostStatus that = (HostStatus) o;
    return hostAlive == that.hostAlive
        && lastStatusUpdateMs == that.lastStatusUpdateMs;
  }


  @Override
  public int hashCode() {
    return Objects.hash(hostAlive, lastStatusUpdateMs);
  }

  @Override
  public String toString() {
    return "HostStatus{"
        + "hostAlive=" + hostAlive
        + ", lastStatusUpdateMs=" + lastStatusUpdateMs
        + '}';
  }
}