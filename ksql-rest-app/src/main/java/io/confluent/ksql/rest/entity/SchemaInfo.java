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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaInfo {

  private final SqlBaseType type;
  private final List<FieldInfo> fields;
  private final SchemaInfo memberSchema;

  @JsonCreator
  public SchemaInfo(
      @JsonProperty("type") final SqlBaseType type,
      @JsonProperty("fields") final List<? extends FieldInfo> fields,
      @JsonProperty("memberSchema") final SchemaInfo memberSchema) {
    Objects.requireNonNull(type);
    this.type = type;
    this.fields = fields == null
        ? null
        : ImmutableList.copyOf(fields);
    this.memberSchema = memberSchema;
  }

  public SqlBaseType getType() {
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
  public boolean equals(final Object other) {
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
