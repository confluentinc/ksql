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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

public class ConfigInfos {
  @JsonProperty("name")
  private final String name;
  @JsonProperty("configs")
  private final ImmutableList<ConfigInfo> configs;

  @JsonCreator
  public ConfigInfos(
      @JsonProperty("name") final String name,
      @JsonProperty("configs") final List<ConfigInfo> configs
  ) {
    this.name = name;
    this.configs = ImmutableList.copyOf(configs);
  }

  @JsonProperty
  public String name() {
    return this.name;
  }

  @JsonProperty("configs")
  public List<ConfigInfo> values() {
    return this.configs;
  }

  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      final ConfigInfos that = (ConfigInfos) o;
      return Objects.equals(this.name, that.name)
          && Objects.equals(this.configs, that.configs);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.name, this.configs});
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[").append(this.name).append(",").append(this.configs).append("]");
    return sb.toString();
  }

  public static class ConfigInfo {
    private final ConfigValueInfo configValue;

    @JsonCreator
    public ConfigInfo(
        @JsonProperty("value") final ConfigValueInfo configValue
    ) {
      this.configValue = configValue;
    }

    @JsonProperty("value")
    public ConfigValueInfo configValue() {
      return this.configValue;
    }

    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        final ConfigInfo that = (ConfigInfo) o;
        return Objects.equals(this.configValue, that.configValue);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.configValue});
    }

    public String toString() {
      return "[" + this.configValue + "]";
    }
  }

  public static class ConfigValueInfo {
    private final String name;
    private final ImmutableList<String> errors;

    @JsonCreator
    public ConfigValueInfo(
        @JsonProperty("name") final String name,
        @JsonProperty("errors") final List<String> errors
    ) {
      this.name = name;
      this.errors = ImmutableList.copyOf(errors);
    }

    @JsonProperty
    public String name() {
      return this.name;
    }

    @JsonProperty
    public List<String> errors() {
      return this.errors;
    }

    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        final ConfigValueInfo that = (ConfigValueInfo) o;
        return Objects.equals(this.name, that.name) && Objects.equals(this.errors, that.errors);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.name, this.errors});
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[").append(this.name).append(",").append(this.errors).append("]");
      return sb.toString();
    }
  }
}
