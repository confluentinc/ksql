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
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertiesList extends KsqlEntity {
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Property {
    private final String name;
    private final String scope;
    private final String value;

    @JsonCreator
    public Property(
        @JsonProperty("name") final String name,
        @JsonProperty("scope") final String scope,
        @JsonProperty("value") final String value
    ) {
      this.name = name;
      this.scope = scope;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getScope() {
      return scope;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      final Property that = (Property) object;
      return Objects.equals(name, that.name)
          && Objects.equals(scope, that.scope)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, scope, value);
    }

    @Override
    public String toString() {
      return "Property{"
          + "name='" + name + '\''
          + ", scope='" + scope + '\''
          + ", value='" + value + '\''
          + '}';
    }
  }

  private final List<Property> properties;
  private final List<String> overwrittenProperties;
  private final List<String> defaultProperties;

  @JsonCreator
  public PropertiesList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("properties") final List<Property> properties,
      @JsonProperty("overwrittenProperties") final List<String> overwrittenProperties,
      @JsonProperty("defaultProperties") final List<String> defaultProperties
  ) {
    super(statementText);
    this.properties = properties == null
        ? Collections.emptyList() : properties;
    this.overwrittenProperties = overwrittenProperties == null
        ? Collections.emptyList() : overwrittenProperties;
    this.defaultProperties = defaultProperties == null
        ? Collections.emptyList() : defaultProperties;
  }

  public List<Property> getProperties() {
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
