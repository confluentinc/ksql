/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ArgumentInfo {

  private final String name;
  private final String type;
  private final String description;

  @JsonCreator
  public ArgumentInfo(@JsonProperty("name") final String name,
                      @JsonProperty("type") final String type,
                      @JsonProperty("description") final String description
  ) {
    this.name = name == null ? "" : name;
    this.type = Objects.requireNonNull(type, "type");
    this.description = description == null ? "" : description;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ArgumentInfo that = (ArgumentInfo) o;
    return Objects.equals(name, that.name)
           && Objects.equals(type, that.type)
           && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description);
  }

  @Override
  public String toString() {
    return "ArgumentInfo{"
           + "name='" + name + '\''
           + ", type='" + type + '\''
           + ", description='" + description + '\''
           + '}';
  }
}
