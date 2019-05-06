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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertiesList extends KsqlEntity {
  private final Map<String, ?> properties;
  private final List<String> overwrittenProperties;
  private final List<String> defaultProperties;

  @JsonCreator
  public PropertiesList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("properties") final Map<String, ?> properties,
      @JsonProperty("overwrittenProperties") final List<String> overwrittenProperties,
      @JsonProperty("defaultProperties") final List<String> defaultProperties
  ) {
    super(statementText);
    this.properties = properties == null
        ? Collections.emptyMap() : properties;
    this.overwrittenProperties = overwrittenProperties == null
        ? Collections.emptyList() : overwrittenProperties;
    this.defaultProperties = defaultProperties == null
        ? Collections.emptyList() : defaultProperties;
  }

  public Map<String, ?> getProperties() {
    return properties;
  }

  public List<String> getOverwrittenProperties() {
    return overwrittenProperties;
  }

  public List<String> getDefaultProperties() {
    return defaultProperties;
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof PropertiesList
        && Objects.equals(properties, ((PropertiesList)o).properties)
        && Objects.equals(overwrittenProperties, ((PropertiesList)o).overwrittenProperties)
        && Objects.equals(defaultProperties, ((PropertiesList)o).defaultProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, overwrittenProperties, defaultProperties);
  }
}
