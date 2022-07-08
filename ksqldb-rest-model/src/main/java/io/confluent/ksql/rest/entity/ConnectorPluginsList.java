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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class ConnectorPluginsList extends KsqlEntity {
  private final ImmutableList<SimpleConnectorPluginInfo> connectorPlugins;

  @JsonCreator
  public ConnectorPluginsList(
      @JsonProperty("statementText")  final String statementText,
      @JsonProperty("warnings")       final List<KsqlWarning> warnings,
      @JsonProperty("connectorsPlugins")     final List<SimpleConnectorPluginInfo> connectorPlugins
  ) {
    super(statementText, warnings);
    this.connectorPlugins =
        ImmutableList.copyOf(Objects.requireNonNull(connectorPlugins, "connectorPlugins"));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "connectorPlugins is ImmutableList")
  public ImmutableList<SimpleConnectorPluginInfo> getConnectorsPlugins() {
    return connectorPlugins;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConnectorPluginsList that = (ConnectorPluginsList) o;
    return Objects.equals(connectorPlugins, that.connectorPlugins);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorPlugins);
  }
}
