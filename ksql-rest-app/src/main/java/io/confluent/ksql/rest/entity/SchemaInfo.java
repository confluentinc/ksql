/**
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
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SchemaInfo {
  public enum Type {
    INTEGER,
    BIGINT,
    DOUBLE,
    BOOLEAN,
    STRING,
    MAP,
    ARRAY,
    STRUCT
  }

  private final Type type;
  private final List<FieldInfo> fields;
  private final SchemaInfo memberSchema;

  @JsonCreator
  public SchemaInfo(
      @JsonProperty("type") final Type type,
      @JsonProperty("fields") final List<FieldInfo> fields,
      @JsonProperty("memberSchema") final SchemaInfo memberSchema) {
    Objects.requireNonNull(type);
    this.type = type;
    this.fields = fields;
    this.memberSchema = memberSchema;
  }

  public Type getType() {
    return type;
  }

  @JsonProperty("type")
  public String getTypeName() {
    return type.name();
  }

  public Optional<List<FieldInfo>> getFields() {
    return Optional.ofNullable(fields);
  }

  public Optional<SchemaInfo> getMemberSchema() {
    return Optional.ofNullable(memberSchema);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SchemaInfo
        && Objects.equals(type, ((SchemaInfo)other).type)
        && Objects.equals(fields, ((SchemaInfo)other).fields)
        && Objects.equals(memberSchema, ((SchemaInfo)other).memberSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, fields, memberSchema);
  }
}
