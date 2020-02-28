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

package io.confluent.support.metrics.common;

import org.apache.avro.generic.GenericContainer;

public abstract class Collector {

  public enum RuntimeState {

    Stopped(0), ShuttingDown(1), Running(2);

    private final int stateId;

    RuntimeState(final int stateId) {
      this.stateId = stateId;
    }

    public int stateId() {
      return stateId;
    }

  }

  private RuntimeState runtimeState;

  public Collector() {
    this.runtimeState = RuntimeState.Stopped;
  }

  /**
   * Collects metrics from a Kafka broker.
   *
   * @return An Avro record that contains the collected metrics.
   */
  public abstract GenericContainer collectMetrics();

  /**
   * Gets the runtime state of this collector.
   */
  public RuntimeState getRuntimeState() {
    return runtimeState;
  }

  /**
   * Sets the runtime state of this collector.
   */
  public void setRuntimeState(final RuntimeState runtimeState) {
    this.runtimeState = runtimeState;
  }

}
