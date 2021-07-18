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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.ConnectorDescription;
import io.confluent.ksql.api.client.ConnectorType;
import java.util.List;
import java.util.Objects;

public class ConnectorDescriptionImpl implements ConnectorDescription {

  private final String name;
  private final String className;
  private final ImmutableList<String> sources;
  private final ImmutableList<String> topics;
  private final ConnectorType type;
  private final String state;

  public ConnectorDescriptionImpl(
      final String name,
      final String className,
      final List<String> sources,
      final List<String> topics,
      final ConnectorType type,
      final String state

  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.className = Objects.requireNonNull(className, "className");
    this.sources = ImmutableList.copyOf(Objects.requireNonNull(sources, "sources"));
    this.topics = ImmutableList.copyOf(Objects.requireNonNull(topics, "topics"));
    this.type = Objects.requireNonNull(type, "type");
    this.state = Objects.requireNonNull(state, "state");
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "sources is ImmutableList")
  public List<String> sources() {
    return sources;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "topics is ImmutableList")
  public List<String> topics() {
    return topics;
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
    final ConnectorDescriptionImpl connectorDescription = (ConnectorDescriptionImpl) o;
    return name.equals(connectorDescription.name)
        && className.equals(connectorDescription.className)
        && sources.equals(connectorDescription.sources)
        && topics.equals(connectorDescription.topics)
        && type.equals(connectorDescription.type)
        && state.equals(connectorDescription.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, sources, topics, type, state);
  }

  @Override
  public String toString() {
    return "ConnectorDescription{"
        + "name='" + name + '\''
        + "className='" + className + '\''
        + ", sources=" + sources
        + ", topics=" + topics
        + ", type=" + type
        + ", state=" + state
        + '}';
  }
}
