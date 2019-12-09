/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HostStatusEntity {

  private HostInfoEntity hostInfoEntity;
  private boolean hostAlive;
  private long lastStatusUpdateMs;

  @JsonCreator
  public HostStatusEntity(
      @JsonProperty("hostInfoEntity") final HostInfoEntity hostInfoEntity,
      @JsonProperty("hostAlive") final boolean hostAlive,
      @JsonProperty("lastStatusUpdateMs") final long lastStatusUpdateMs
  ) {
    this.hostInfoEntity = Objects.requireNonNull(hostInfoEntity, "hostInfoEntity");
    this.hostAlive = hostAlive;
    this.lastStatusUpdateMs = lastStatusUpdateMs;
  }

  public HostInfoEntity getHostInfoEntity() {
    return hostInfoEntity;
  }

  public boolean getHostAlive() {
    return hostAlive;
  }

  public long getLastStatusUpdateMs() {
    return lastStatusUpdateMs;
  }

  public void setHostInfoEntity(final HostInfoEntity hostInfoEntity) {
    this.hostInfoEntity = hostInfoEntity;
  }

  public void setHostAlive(final boolean hostAlive) {
    this.hostAlive = hostAlive;
  }

  public void setLastStatusUpdateMs(final long lastStatusUpdateMs) {
    this.lastStatusUpdateMs = lastStatusUpdateMs;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostStatusEntity that = (HostStatusEntity) o;
    return Objects.equals(hostInfoEntity, that.hostInfoEntity)
        && hostAlive == that.hostAlive && lastStatusUpdateMs == that.lastStatusUpdateMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostInfoEntity, hostAlive, lastStatusUpdateMs);
  }

  @Override
  public String toString() {
    return hostInfoEntity + "," + hostAlive + "," + lastStatusUpdateMs;
  }
}
