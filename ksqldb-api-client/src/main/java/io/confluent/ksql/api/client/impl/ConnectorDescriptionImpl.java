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

import io.confluent.ksql.api.client.ConnectorDescription;
import io.confluent.ksql.api.client.ConnectorType;
import java.util.List;
import java.util.Objects;

public class ConnectorDescriptionImpl implements ConnectorDescription {

  private final String connectorClass;
  private final List<String> sources;
  private final List<String> topics;
  private final ConnectorType type;
  private final String state;

  public ConnectorDescriptionImpl(
      final String connectorClass,
      final List<String> sources,
      final List<String> topics,
      final ConnectorType type,
      final String state

  ) {
    this.connectorClass = Objects.requireNonNull(connectorClass);
    this.sources = Objects.requireNonNull(sources);
    this.topics = Objects.requireNonNull(topics);
    this.type = Objects.requireNonNull(type);
    this.state = Objects.requireNonNull(state);
  }


  @Override
  public String connectorClass() {
    return connectorClass;
  }

  @Override
  public List<String> sources() {
    return sources;
  }

  @Override
  public List<String> topics() {
    return topics;
  }

  @Override
  public ConnectorType type() {
    return type;
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
    final ConnectorDescriptionImpl connectorDescription = (ConnectorDescriptionImpl) o;
    return connectorClass.equals(connectorDescription.connectorClass)
        && sources.equals(connectorDescription.sources)
        && topics.equals(connectorDescription.topics)
        && type.equals(connectorDescription.type)
        && state.equals(connectorDescription.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorClass, sources, topics, type, state);
  }

  @Override
  public String toString() {
    return "ConnectorDescription{"
        + "connectorClass='" + connectorClass + '\''
        + ", sources=" + sources
        + ", topics=" + topics
        + ", type=" + type
        + ", state=" + state
        + '}';
  }
}
