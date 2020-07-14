/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SimpleFunctionInfo implements Comparable<SimpleFunctionInfo> {

  private final String name;
  private final FunctionType type;
  private final String category;

  @JsonCreator
  public SimpleFunctionInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("type") final FunctionType type,
      @JsonProperty("category") final String category
  ) {
    this.name = Objects.requireNonNull(name, "name can't be null");
    this.type = Objects.requireNonNull(type, "type can't be null");
    this.category = Objects.isNull(category) ? "" : category;
  }

  public String getName() {
    return name;
  }

  public FunctionType getType() {
    return type;
  }

  public String getCategory() {
    return category;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleFunctionInfo that = (SimpleFunctionInfo) o;
    return Objects.equals(name, that.name)
        && type == that.type
        && Objects.equals(category, that.category);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, category);
  }

  @Override
  public String toString() {
    return "SimpleFunctionInfo{"
        + "name='" + name + '\''
        + ", type='" + type + '\''
        + ", category=" + category
        + '}';
  }

  @Override
  public int compareTo(final SimpleFunctionInfo other) {
    return name.compareTo(other.name);
  }
}
