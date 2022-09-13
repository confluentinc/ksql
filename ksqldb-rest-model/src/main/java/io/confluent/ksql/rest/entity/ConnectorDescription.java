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
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorDescription extends KsqlEntity {

  private final String connectorClass;
  private final ConnectorStateInfo status;
  private final ImmutableList<SourceDescription> sources;
  private final ImmutableList<String> topics;

  @JsonCreator
  public ConnectorDescription(
      @JsonProperty("statementText")  final String statementText,
      @JsonProperty("connectorClass") final String connectorClass,
      @JsonProperty("status")         final ConnectorStateInfo status,
      @JsonProperty("sources")        final List<SourceDescription> sources,
      @JsonProperty("topics")         final List<String> topics,
      @JsonProperty("warnings")       final List<KsqlWarning> warnings
  ) {
    super(statementText, warnings);
    this.connectorClass = Objects.requireNonNull(connectorClass, "connectorClass");
    this.status = Objects.requireNonNull(status, "status");
    this.sources = ImmutableList.copyOf(Objects.requireNonNull(sources, "sources"));
    this.topics = ImmutableList.copyOf(Objects.requireNonNull(topics, "topics"));
  }

  public String getConnectorClass() {
    return connectorClass;
  }

  public ConnectorStateInfo getStatus() {
    return status;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "sources is ImmutableList")
  public List<SourceDescription> getSources() {
    return sources;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "topics is ImmutableList")
  public List<String> getTopics() {
    return topics;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConnectorDescription that = (ConnectorDescription) o;
    return Objects.equals(status, that.status)
        && Objects.equals(connectorClass, that.connectorClass)
        && Objects.equals(sources, that.sources)
        && Objects.equals(topics, that.topics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, sources, connectorClass, topics);
  }

  @Override
  public String toString() {
    return "ConnectorDescription{"
        + "status=" + status
        + ", sources=" + sources
        + ", connectorClass='" + connectorClass + "'"
        + ", topics='" + topics + "'"
        + '}';
  }
}
