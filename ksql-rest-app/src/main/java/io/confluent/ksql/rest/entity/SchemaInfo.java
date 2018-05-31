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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

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
      @JsonProperty("type") String type,
      @JsonProperty("fields") List<FieldInfo> fields,
      @JsonProperty("memberSchema") SchemaInfo memberSchema) {
    this.type = Type.valueOf(type);
    this.fields = fields == null ? null : ImmutableList.copyOf(fields);
    this.memberSchema = memberSchema;
  }

  public SchemaInfo(Type type, List<FieldInfo> fields, SchemaInfo memberSchema) {
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

  public List<FieldInfo> getFields() {
    return fields;
  }

  public SchemaInfo getMemberSchema() {
    return memberSchema;
  }

  @Override
  public boolean equals(Object other) {
    return
        other instanceof SchemaInfo
        && Objects.equals(type, ((SchemaInfo)other).type)
        && Objects.equals(fields, ((SchemaInfo)other).fields)
        && Objects.equals(memberSchema, ((SchemaInfo)other).memberSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, fields, memberSchema);
  }
}
