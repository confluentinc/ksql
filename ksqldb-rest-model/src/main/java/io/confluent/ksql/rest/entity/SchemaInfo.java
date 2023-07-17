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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)

public class SchemaInfo {

  private final SqlBaseType type;
  private final ImmutableList<FieldInfo> fields;
  private final SchemaInfo memberSchema;
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @EffectivelyImmutable
  private final ImmutableMap<String, Object> parameters;

  @JsonCreator
  public SchemaInfo(
      @JsonProperty("type") final SqlBaseType type,
      @JsonProperty("fields") final List<? extends FieldInfo> fields,
      @JsonProperty("memberSchema") final SchemaInfo memberSchema,
      @JsonProperty("parameters") final ImmutableMap<String, Object> parameters) {
    Objects.requireNonNull(type);
    this.type = type;
    this.fields = fields == null
        ? null
        : ImmutableList.copyOf(fields);
    this.memberSchema = memberSchema;
    this.parameters = parameters;

  }

  public SchemaInfo(
      @JsonProperty("type") final SqlBaseType type,
      @JsonProperty("fields") final List<? extends FieldInfo> fields,
      @JsonProperty("memberSchema") final SchemaInfo memberSchema) {
    this(type, fields, memberSchema, ImmutableMap.of());
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

  public ImmutableMap<String, Object> getParameters() {
    return parameters;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SchemaInfo that = (SchemaInfo) o;
    return type == that.type
        && Objects.equals(fields, that.fields)
        && Objects.equals(memberSchema, that.memberSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, fields, memberSchema);
  }

  @EffectivelyImmutable
  private static final ImmutableMap<SqlBaseType, Function<SchemaInfo, String>> TO_TYPE_STRING =
      ImmutableMap.<SqlBaseType, Function<SchemaInfo, String>>builder()
          .put(SqlBaseType.STRING, si -> "VARCHAR(STRING)")
          .put(
              SqlBaseType.ARRAY,
              si -> SqlBaseType.ARRAY + "<" + si.memberSchema.toTypeString() + ">")
          .put(
              SqlBaseType.MAP,
              si -> SqlBaseType.MAP
                  + "<" + SqlBaseType.STRING
                  + ", " + si.memberSchema.toTypeString()
                  + ">")
          .put(
              SqlBaseType.STRUCT,
              si -> si.fields
                  .stream()
                  .map(f -> f.getName() + " " + f.getSchema().toTypeString())
                  .collect(Collectors.joining(", ", SqlBaseType.STRUCT + "<", ">")))
          .put(
              SqlBaseType.DECIMAL,
              si -> {
                // Backwards compatibility case when the backend does not return parameters
                if (si.getParameters().isEmpty()) {
                  return String.valueOf(SqlBaseType.DECIMAL);
                }

                final Object precision = si.getParameters().get(SqlDecimal.PRECISION);
                final Object scale = si.getParameters().get(SqlDecimal.SCALE);
                final String parameterString = String.format("(%s, %s)", precision, scale);
                KsqlPreconditions.checkArgument(
                    (precision != null & scale != null),
                    "Either one of precision and scale missing: "
                        + parameterString
                );
                return SqlBaseType.DECIMAL + parameterString;
              })
          .build();

  public String toTypeString() {
    // needs a map instead of switch because for some reason switch creates an
    // internal class with no annotations that messes up EntityTest
    return Optional.ofNullable(TO_TYPE_STRING.getOrDefault(type, si -> si.type.name()))
        .orElseThrow(NullPointerException::new).apply(this);
  }
}
