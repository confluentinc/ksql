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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class SimpleConfigInfos {
  @JsonProperty("configs")
  private final ImmutableList<SimpleConfigInfo> configs;

  @JsonCreator
  public SimpleConfigInfos(
      @JsonProperty("configs") final ImmutableList<SimpleConfigInfo> configs
  ) {
    this.configs = configs;
  }

  @JsonProperty("configs")
  public List<SimpleConfigInfo> values() {
    return this.configs;
  }

  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleConfigInfos that = (SimpleConfigInfos) o;
    return Objects.equals(configs, that.configs);
  }

  public int hashCode() {
    return Objects.hash(configs);
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[").append(this.configs).append("]");
    return sb.toString();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(Include.NON_NULL)
  public static class SimpleConfigInfo {
    private final SimpleConfigValueInfo configValue;

    @JsonCreator
    public SimpleConfigInfo(@JsonProperty("value") final SimpleConfigValueInfo configValue) {
      this.configValue = configValue;
    }


    @JsonProperty("value")
    public SimpleConfigValueInfo configValue() {
      return this.configValue;
    }

    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SimpleConfigInfo that = (SimpleConfigInfo) o;
      return Objects.equals(configValue, that.configValue);
    }

    public int hashCode() {
      return Objects.hash(this.configValue);
    }

    public String toString() {
      return "[" + this.configValue + "]";
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(Include.NON_NULL)
  public static class SimpleConfigValueInfo {
    private final String name;
    private final ImmutableList<String> errors;

    @JsonCreator
    public SimpleConfigValueInfo(
        @JsonProperty("name") final String name,
        @JsonProperty("errors") final ImmutableList<String> errors
    ) {
      this.name = name;
      this.errors = errors;
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
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SimpleConfigValueInfo that = (SimpleConfigValueInfo) o;
      return Objects.equals(name, that.name)
          && Objects.equals(errors, that.errors);
    }

    public int hashCode() {
      return Objects.hash(this.name, this.errors);
    }

    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[").append(this.name).append(",").append(this.errors).append("]");
      return sb.toString();
    }
  }
}
