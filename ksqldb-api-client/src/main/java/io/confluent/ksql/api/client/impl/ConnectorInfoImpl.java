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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ConnectorInfo;
import io.confluent.ksql.api.client.ConnectorType;
import java.util.Objects;

public class ConnectorInfoImpl implements ConnectorInfo {

  private final String name;
  private final ConnectorType type;
  private final String className;
  private final String state;

  public ConnectorInfoImpl(
      final String name,
      final ConnectorType type,
      final String className,
      final String state
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.className = Objects.requireNonNull(className, "className");
    this.state = Objects.requireNonNull(state, "state");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public ConnectorType type() {
    return type;
  }

  @Override
  public String className() {
    return className;
  }

  @Override
  public String state() {
    return state;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConnectorInfoImpl connectorInfo = (ConnectorInfoImpl) o;
    return name.equals(connectorInfo.name)
        && type.equals(connectorInfo.type)
        && className.equals(connectorInfo.className)
        && state.equals(connectorInfo.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, className, state);
  }

  @Override
  public String toString() {
    return "ConnectorInfo{"
        + "name='" + name + '\''
        + ", type=" + type
        + ", className=" + className
        + ", state=" + state
        + '}';
  }
}
