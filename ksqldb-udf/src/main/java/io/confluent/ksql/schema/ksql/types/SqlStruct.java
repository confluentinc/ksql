/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql.types;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Immutable
public final class SqlStruct extends SqlType {

  private static final String PREFIX = "STRUCT<";
  private static final String POSTFIX = ">";
  private static final String EMPTY_STRUCT = PREFIX + " " + POSTFIX;

  private final ImmutableList<Field> fields;
  private final ImmutableMap<String, Field> byName;

  public static Builder builder() {
    return new Builder();
  }

  private SqlStruct(final List<Field> fields, final Map<String, Field> byName) {
    super(SqlBaseType.STRUCT);
    this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields"));
    this.byName = ImmutableMap.copyOf(requireNonNull(byName, "byName"));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "fields is ImmutableList")
  public List<Field> fields() {
    return fields;
  }

  public Optional<Field> field(final String name) {
    return Optional.ofNullable(byName.get(name));
  }

  public Optional<UnionType> unionType() {
    // Union/AnyOf types in Schema Registry are encoded in Connect as a Struct with a special
    // schema name: io.confluent.connect.json.OneOf and one field for each of the alternative types.
    //
    // {"anyOf":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]
    //
    // The fields for each alternative types are named as following:
    // io.confluent.connect.json.OneOf.field.0 - String type
    // io.confluent.connect.json.OneOf.field.1 - Integer type
    // io.confluent.connect.json.OneOf.field.2 - boolean type
    //
    // During Ser/De in the JsonSchemaConverter the first field with a non-null value is picked as
    // the value of the current Union/AnyOf type. (@see Class JsonSchemaData in Schema Registry).
    //
    // KsqlDB schema in the Command object does not maintain this special information for the
    // Structs (nor does it store the names associated with the field schemas) and hence it ends
    // up treating it as a regular struct thereby changing its interpretation during serialization.
    // This results in Schema mismatch errors.
    //
    // To handle this, If all the fields in this struct follow the above naming convention,
    // we infer this Struct to be of a Union type.
    //
    // While preparing the Connect record for Serialization, we explicitly set the schema name as
    // io.confluent.connect.json.OneOf if the current Struct is of Union type.
    // @see io.confluent.ksql.schema.ksql.SchemaConverters

    // There is another way in which Union type is defined in the format
    // connect_union_field_1,
    // connect_union_field_2,
    // connect_union_field_3......
    // This format is used when generalized.sum.type.support is true (default is false)
    if (UnionType.ONE_OF_TYPE.matches(fields)) {
      return Optional.of(UnionType.ONE_OF_TYPE);
    } else if (UnionType.GENERALIZED_TYPE.matches(fields)) {
      return Optional.of(UnionType.GENERALIZED_TYPE);
    } else {
      return Optional.empty();
    }
  }

  public enum UnionType {
    GENERALIZED_TYPE {
      @Override
      public boolean matches(final ImmutableList<Field> fields) {
        final Predicate<Field> isConnectUnionTypeField =
            field -> field.name.startsWith(JsonSchemaData.GENERALIZED_TYPE_UNION_FIELD_PREFIX);
        return !fields.isEmpty() && fields.stream().allMatch(isConnectUnionTypeField);
      }
    },
    ONE_OF_TYPE {
      @Override
      public boolean matches(final ImmutableList<Field> fields) {
        final Predicate<Field> isOneOfUnionTypeField =
            field -> field.name.startsWith(JsonSchemaData.JSON_TYPE_ONE_OF + ".field");
        return !fields.isEmpty() && fields.stream().allMatch(isOneOfUnionTypeField);
      }
    };

    public abstract boolean matches(ImmutableList<Field> fields);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlStruct struct = (SqlStruct) o;
    return fields.equals(struct.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    if (fields.isEmpty()) {
      return EMPTY_STRUCT;
    }

    return fields.stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", ", PREFIX, POSTFIX));
  }

  public static final class Builder {

    private final List<Field> fields = new ArrayList<>();
    private final Map<String, Field> byName = new HashMap<>();

    public Builder field(final String fieldName, final SqlType fieldType) {
      final Field field = new Field(fieldName, fieldType, fields.size());
      if (byName.putIfAbsent(field.name(), field) != null) {
        throw new DataException("Duplicate field names found in STRUCT: "
            + "'" + byName.get(field.name()) + "' and '" + field + "'");
      }

      fields.add(field);
      return this;
    }

    public Builder field(final Field field) {
      field(field.name(), field.type());
      return this;
    }

    public Builder fields(final Iterable<? extends Field> fields) {
      fields.forEach(this::field);
      return this;
    }

    public SqlStruct build() {
      return new SqlStruct(fields, byName);
    }
  }

  @Immutable
  public static final class Field {

    private final String name;
    private final SqlType type;
    private final int index;

    @VisibleForTesting
    Field(final String name, final SqlType type, final int index) {
      this.name = requireNonNull(name, "name");
      this.type = requireNonNull(type, "type");
      this.index = index;

      if (!name.trim().equals(name)) {
        throw new IllegalArgumentException("name is not trimmed: '" + name + "'");
      }

      if (name.isEmpty()) {
        throw new IllegalArgumentException("name is empty");
      }

      if (index < 0) {
        throw new IllegalArgumentException("negative index: " + index);
      }
    }

    public SqlType type() {
      return type;
    }

    public String name() {
      return name;
    }

    /**
     * @return The index of the field within its parent struct.
     */
    public int index() {
      return index;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Field that = (Field) o;
      return index == that.index
          && Objects.equals(name, that.name)
          && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(index, name, type);
    }

    @Override
    public String toString() {
      return toString(FormatOptions.none());
    }

    public String toString(final FormatOptions formatOptions) {
      return formatOptions.escape(name) + " " + type.toString(formatOptions);
    }
  }
}
