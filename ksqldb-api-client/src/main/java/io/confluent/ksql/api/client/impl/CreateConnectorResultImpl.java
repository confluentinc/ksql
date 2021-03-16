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

import io.confluent.ksql.api.client.ConnectorType;
import io.confluent.ksql.api.client.CreateConnectorResult;
import java.util.Map;
import java.util.Objects;

public class CreateConnectorResultImpl implements CreateConnectorResult {

  private final String name;
  private final ConnectorType type;
  private final Map<String, String> properties;

  public CreateConnectorResultImpl(
      final String name,
      final ConnectorType type,
      final Map<String, String> properties
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.properties = Objects.requireNonNull(properties, "properties");
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
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateConnectorResultImpl connectorInfo = (CreateConnectorResultImpl) o;
    return name.equals(connectorInfo.name)
        && type.equals(connectorInfo.type)
        && properties.equals(connectorInfo.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, properties);
  }

  @Override
  public String toString() {
    return "CreateConnectorResult{"
        + "name='" + name + '\''
        + ", type=" + type
        + ", properties=" + properties
        + '}';
  }
}
