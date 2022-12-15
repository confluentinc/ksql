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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Immutable
public class SimpleConnectorPluginInfo {
  private final String className;
  private final ConnectorType type;
  private final String version;

  @JsonCreator
  public SimpleConnectorPluginInfo(
      @JsonProperty("className") final String className,
      @JsonProperty("type") final ConnectorType type,
      @JsonProperty("version") final String version
  ) {
    this.className = Objects.requireNonNull(className, "className");
    this.type = type;
    this.version = version;
  }

  public String getClassName() {
    return className;
  }

  public ConnectorType getType() {
    return type;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleConnectorPluginInfo that = (SimpleConnectorPluginInfo) o;
    return Objects.equals(className, that.className)
        && type == that.type
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, type, version);
  }

  @Override
  public String toString() {
    return "SimpleConnectorPluginInfo{"
        + "className='" + className + '\''
        + ", type=" + type
        + ", version='" + version + '\''
        + '}';
  }
}
