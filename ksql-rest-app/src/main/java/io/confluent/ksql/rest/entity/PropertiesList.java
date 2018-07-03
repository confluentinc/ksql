/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PropertiesList extends KsqlEntity {
  private final Map<String, ?> properties;
  private final List<String> overwrittenProperties;

  @JsonCreator
  public PropertiesList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("properties") Map<String, ?> properties,
      @JsonProperty("overwrittenProperties") List<String> overwrittenProperties
  ) {
    super(statementText);
    this.properties = properties;
    this.overwrittenProperties = overwrittenProperties;
  }

  public Map<String, ?> getProperties() {
    return properties;
  }

  public List<String> getOverwrittenProperties() {
    return overwrittenProperties;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PropertiesList
        && Objects.equals(properties, ((PropertiesList)o).properties)
        && Objects.equals(overwrittenProperties, ((PropertiesList)o).overwrittenProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, overwrittenProperties);
  }
}
