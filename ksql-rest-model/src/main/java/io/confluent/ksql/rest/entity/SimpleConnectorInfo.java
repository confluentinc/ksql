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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class SimpleConnectorInfo {

  private final String name;
  private final ConnectorType type;
  private final String className;

  @JsonCreator
  public SimpleConnectorInfo(
      @JsonProperty("name")       final String name,
      @JsonProperty("type")       final ConnectorType type,
      @JsonProperty("className")  final String className
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.type = type;
    this.className = className;
  }

  public String getName() {
    return name;
  }

  public ConnectorType getType() {
    return type;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleConnectorInfo that = (SimpleConnectorInfo) o;
    return Objects.equals(name, that.name)
        && type == that.type
        && Objects.equals(className, that.className);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, className);
  }

  @Override
  public String toString() {
    return "SimpleConnectorInfo{"
        + "name='" + name + '\''
        + ", type=" + type
        + ", className='" + className + '\''
        + '}';
  }
}
