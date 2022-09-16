/*
 * Copyright 2022 Confluent Inc.
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
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorInfo {
  private final String name;
  private final ImmutableMap<String, String> config;
  private final ConnectorType type;

  @JsonCreator
  public ConnectorInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("config") final Map<String, String> config,
      @JsonProperty("type") final ConnectorType type
  ) {
    this.name = name;
    this.config = ImmutableMap.copyOf(config);
    this.type = type;
  }

  @JsonProperty
  public String name() {
    return this.name;
  }

  @JsonProperty
  public ConnectorType type() {
    return this.type;
  }

  @JsonProperty
  public Map<String, String> config() {
    return this.config;
  }

  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      final ConnectorInfo that = (ConnectorInfo)o;
      return Objects.equals(this.name, that.name)
          && Objects.equals(this.config, that.config)
          && Objects.equals(this.type, that.type);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.name, this.config, this.type});
  }
}
