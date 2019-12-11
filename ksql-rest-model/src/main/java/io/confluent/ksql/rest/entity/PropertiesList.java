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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertiesList extends KsqlEntity {
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Property {
    private final String property;
    private final String scope;

    @JsonCreator
    public Property(
        @JsonProperty("property") final String property,
        @JsonProperty("scope") final String scope
    ) {
      this.property = property;
      this.scope = scope;
    }

    public String getProperty() {
      return property;
    }

    public String getScope() {
      return scope;
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      Property that = (Property) object;
      return Objects.equals(property, that.property)
          && Objects.equals(scope, that.scope);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, scope);
    }
  }

  private static class PropertySerializer extends JsonSerializer<Property> {

    @Override
    public void serialize(
        Property value, JsonGenerator generator, SerializerProvider serializers)
        throws IOException {
      generator.writeFieldName(value.property + '-' + value.scope);
    }
  }

  private static class PropertyDeserializer extends KeyDeserializer {
    @Override
    public Property deserializeKey(String key, DeserializationContext ctxt) throws IOException {
      String[] value = key.split("-");
      return new Property(value[0], value[1]);
    }
  }

  private final Map<Property, ?> properties;
  private final List<String> overwrittenProperties;
  private final List<String> defaultProperties;

  @JsonCreator
  public PropertiesList(
      @JsonProperty("statementText") final String statementText,
      @JsonSerialize(keyUsing = PropertySerializer.class)
      @JsonDeserialize(keyUsing = PropertyDeserializer.class)
      @JsonProperty("properties") final Map<Property, ?> properties,
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

  public Map<Property, ?> getProperties() {
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
