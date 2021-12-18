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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
@JsonInclude(Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldInfo {

  public enum FieldType {
    SYSTEM, // To be removed in the future. 0.9 saw this value no longer used.
    KEY,
    HEADER
  }

  private final String name;
  private final SchemaInfo schema;
  private final Optional<FieldType> type;
  private final Optional<String> headerKey;

  public FieldInfo(
      final String name,
      final SchemaInfo schema,
      final Optional<FieldType> type
  ) {
    this(name, schema, type, Optional.empty());
  }

  @JsonCreator
  public FieldInfo(
      @JsonProperty(value = "name", required = true) final String name,
      @JsonProperty(value = "schema", required = true) final SchemaInfo schema,
      @JsonProperty("fieldType") final Optional<FieldType> type,
      @JsonProperty("headerKey") final Optional<String> headerKey
  ) {
    this.name = Objects.requireNonNull(name, "name");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.type = Objects.requireNonNull(type, "type");
    this.headerKey = Objects.requireNonNull(headerKey, "headerKey");
  }

  public String getName() {
    return this.name;
  }

  public SchemaInfo getSchema() {
    return schema;
  }

  public Optional<FieldType> getType() {
    return type;
  }

  public Optional<String> getHeaderKey() {
    return headerKey;
  }

  @Override
  public boolean equals(final Object other) {
    return other instanceof FieldInfo
        && Objects.equals(name, ((FieldInfo) other).name)
        && Objects.equals(schema, ((FieldInfo) other).schema)
        && Objects.equals(type, ((FieldInfo) other).type)
        && Objects.equals(headerKey, ((FieldInfo) other).headerKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, schema, type, headerKey);
  }

  @Override
  public String toString() {
    return "FieldInfo{"
        + "name='" + name + '\''
        + ", schema=" + schema
        + ", type=" + type
        + ", headerKey=" + headerKey
        + '}';
  }
}
