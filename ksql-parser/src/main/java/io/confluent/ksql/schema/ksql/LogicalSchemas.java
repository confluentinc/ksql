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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Util class for converting to and from KSQL's logical schema.
 */
public final class LogicalSchemas {

  public static final Schema BOOLEAN = Schema.OPTIONAL_BOOLEAN_SCHEMA;
  public static final Schema INTEGER = Schema.OPTIONAL_INT32_SCHEMA;
  public static final Schema BIGINT = Schema.OPTIONAL_INT64_SCHEMA;
  public static final Schema DOUBLE = Schema.OPTIONAL_FLOAT64_SCHEMA;
  public static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;

  private static final LogicalToSqlTypeConverter TO_SQL_CONVERTER = new ToSqlTypeConverter();
  private static final SqlTypeToLogicalConverter FROM_SQL_CONVERTER = new FromSqlTypeConverter();

  private LogicalSchemas() {
  }

  public interface LogicalToSqlTypeConverter {
    /**
     * Convert the supplied logical KSQL {@code schema} to its corresponding SQL type.
     *
     * @param schema the logical KSQL schema.
     * @return the sql type.
     */
    Type toSqlType(Schema schema);
  }

  public interface SqlTypeToLogicalConverter {
    /**
     * Convert the supplied primitive {@code sqlType} to its corresponding logical KSQL schema.
     *
     * @param sqlType the sql type to convert
     * @return the logical schema.
     */
    Schema fromSqlType(Type sqlType);
  }

  public static LogicalToSqlTypeConverter toSqlTypeConverter() {
    return TO_SQL_CONVERTER;
  }

  public static SqlTypeToLogicalConverter fromSqlTypeConverter() {
    return FROM_SQL_CONVERTER;
  }

  private static final class ToSqlTypeConverter implements LogicalToSqlTypeConverter {

    private static final Map<Schema.Type, Function<Schema, Type>> LOGICAL_TO_SQL = ImmutableMap
        .<Schema.Type, Function<Schema, Type>>builder()
        .put(Schema.Type.INT32, s -> PrimitiveType.of(Type.SqlType.INTEGER))
        .put(Schema.Type.INT64, s -> PrimitiveType.of(Type.SqlType.BIGINT))
        .put(Schema.Type.FLOAT32, s -> PrimitiveType.of(Type.SqlType.DOUBLE))
        .put(Schema.Type.FLOAT64, s -> PrimitiveType.of(Type.SqlType.DOUBLE))
        .put(Schema.Type.BOOLEAN, s -> PrimitiveType.of(Type.SqlType.BOOLEAN))
        .put(Schema.Type.STRING, s -> PrimitiveType.of(Type.SqlType.STRING))
        .put(Schema.Type.ARRAY, ToSqlTypeConverter::toSqlArray)
        .put(Schema.Type.MAP, ToSqlTypeConverter::toSqlMap)
        .put(Schema.Type.STRUCT, ToSqlTypeConverter::toSqlStruct)
        .build();

    @Override
    public Type toSqlType(final Schema schema) {
      return sqlType(schema);
    }

    private static Type sqlType(final Schema schema) {
      final Function<Schema, Type> handler = LOGICAL_TO_SQL.get(schema.type());
      if (handler == null) {
        throw new KsqlException("Unexpected logical type: " + schema);
      }

      return handler.apply(schema);
    }

    private static Array toSqlArray(final Schema schema) {
      return io.confluent.ksql.parser.tree.Array.of(sqlType(schema.valueSchema()));
    }

    private static Type toSqlMap(final Schema schema) {
      if (schema.keySchema().type() != Schema.Type.STRING) {
        throw new KsqlException("Unsupported map key type: " + schema.keySchema());
      }
      return io.confluent.ksql.parser.tree.Map.of(sqlType(schema.valueSchema()));
    }

    private static Struct toSqlStruct(final Schema schema) {
      final Struct.Builder builder = Struct.builder();

      schema.schema().fields()
          .forEach(field -> builder.addField(field.name(), sqlType(field.schema())));

      return builder.build();
    }
  }

  private static final class FromSqlTypeConverter implements SqlTypeToLogicalConverter {

    private static final Map<Type.SqlType, Function<Type, Schema>> SQL_TO_LOGICAL = ImmutableMap
        .<Type.SqlType, Function<Type, Schema>>builder()
        .put(Type.SqlType.STRING, t -> STRING)
        .put(Type.SqlType.BOOLEAN, t -> BOOLEAN)
        .put(Type.SqlType.INTEGER, t -> INTEGER)
        .put(Type.SqlType.BIGINT, t -> BIGINT)
        .put(Type.SqlType.DOUBLE, t -> DOUBLE)
        .put(Type.SqlType.ARRAY, t -> FromSqlTypeConverter.fromSqlArray((Array) t))
        .put(Type.SqlType.MAP, t -> FromSqlTypeConverter
            .fromSqlMap((io.confluent.ksql.parser.tree.Map) t))
        .put(Type.SqlType.STRUCT, t -> FromSqlTypeConverter.fromSqlStruct((Struct) t))
        .build();

    @Override
    public Schema fromSqlType(final Type sqlType) {
      return logicalType(sqlType);
    }

    private static Schema logicalType(final Type sqlType) {
      final Function<Type, Schema> handler = SQL_TO_LOGICAL.get(sqlType.getSqlType());
      if (handler == null) {
        throw new KsqlException("Unexpected sql type: " + sqlType);
      }

      return handler.apply(sqlType);
    }

    private static Schema fromSqlArray(final Array sqlType) {
      return SchemaBuilder
          .array(logicalType(sqlType.getItemType()))
          .optional()
          .build();
    }

    private static Schema fromSqlMap(final io.confluent.ksql.parser.tree.Map sqlType) {
      return SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, logicalType(sqlType.getValueType()))
          .optional()
          .build();
    }

    private static Schema fromSqlStruct(final Struct struct) {
      final SchemaBuilder builder = SchemaBuilder.struct();

      struct.getFields()
          .forEach(field -> builder.field(field.getName(), logicalType(field.getType())));

      return builder
          .optional()
          .build();
    }
  }
}
