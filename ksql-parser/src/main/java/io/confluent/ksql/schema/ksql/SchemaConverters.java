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
import io.confluent.ksql.parser.tree.Decimal;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Util class for converting between KSQL's {@link LogicalSchema} and it's SQL types, i.e. those
 * derived from {@link Type}.
 *
 * <p>KSQL the following main type systems / schema types:
 *
 * <ul>
 *   <li>
 *     <b>SQL {@link Type}:</b>
 *     - the SQL type system, e.g. INTEGER, BIGINT, ARRAY&lt;something&gt;, etc
 *   </li>
 *   <li>
 *     <b>{@link LogicalSchema}:</b>
 *     - the set of named {@code SQL} types that represent the schema of one row in a KSQL stream
 *     or table.
 *   </li>
 *   <li>
 *     <b>{@link PhysicalSchema}:</b>
 *     - the schema of how all the row's parts, e.g. key, value, are serialized to/from Kafka.
 *   </li>
 *   <li>
 *     <b>{@link PersistenceSchema}:</b>
 *     - the schema of how one part of the row, e.g. key, value, is serialized to/from Kafka.
 *   </li>
 * </ul>
 */
public final class SchemaConverters {

  public static final Schema BOOLEAN = Schema.OPTIONAL_BOOLEAN_SCHEMA;
  public static final Schema INTEGER = Schema.OPTIONAL_INT32_SCHEMA;
  public static final Schema BIGINT = Schema.OPTIONAL_INT64_SCHEMA;
  public static final Schema DOUBLE = Schema.OPTIONAL_FLOAT64_SCHEMA;
  public static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;

  private static final LogicalToSqlTypeConverter LOGICAL_TO_SQL_CONVERTER =
      new LogicalToSqlConverter();

  private static final SqlToLogicalTypeConverter SQL_TO_LOGICAL_CONVERTER =
      new LogicalFromSqlConverter();

  private static final JavaToSqlTypeConverter JAVA_TO_SQL_CONVERTER =
      new JavaToSqlConverter();

  private SchemaConverters() {
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

  public interface SqlToLogicalTypeConverter {

    /**
     * @see #fromSqlType(Type, String, String)
     */
    Schema fromSqlType(Type sqlType);

    /**
     * Convert the supplied primitive {@code sqlType} to its corresponding logical KSQL schema.
     *
     * @param sqlType the sql type to convert
     * @param name    the name for the schema
     * @param doc     the doc for the schema
     * @return the logical schema.
     */
    Schema fromSqlType(Type sqlType, String name, String doc);
  }

  public interface JavaToSqlTypeConverter {

    /**
     * Convert the supplied {@code javaType} to its corresponding SQL type.
     *
     * <p/>Structured types are not supported
     *
     * @param javaType the java type to convert.
     * @return the sql type.
     */
    SqlType toSqlType(Class<?> javaType);
  }

  public static LogicalToSqlTypeConverter logicalToSqlConverter() {
    return LOGICAL_TO_SQL_CONVERTER;
  }

  public static SqlToLogicalTypeConverter sqlToLogicalConverter() {
    return SQL_TO_LOGICAL_CONVERTER;
  }

  public static JavaToSqlTypeConverter javaToSqlConverter() {
    return JAVA_TO_SQL_CONVERTER;
  }

  private static final class LogicalToSqlConverter implements LogicalToSqlTypeConverter {

    private static final Map<Schema.Type, Function<Schema, Type>> LOGICAL_TO_SQL = ImmutableMap
        .<Schema.Type, Function<Schema, Type>>builder()
        .put(Schema.Type.INT32, s -> PrimitiveType.of(SqlType.INTEGER))
        .put(Schema.Type.INT64, s -> PrimitiveType.of(SqlType.BIGINT))
        .put(Schema.Type.FLOAT32, s -> PrimitiveType.of(SqlType.DOUBLE))
        .put(Schema.Type.FLOAT64, s -> PrimitiveType.of(SqlType.DOUBLE))
        .put(Schema.Type.BOOLEAN, s -> PrimitiveType.of(SqlType.BOOLEAN))
        .put(Schema.Type.STRING, s -> PrimitiveType.of(SqlType.STRING))
        .put(Schema.Type.ARRAY, LogicalToSqlConverter::toSqlArray)
        .put(Schema.Type.MAP, LogicalToSqlConverter::toSqlMap)
        .put(Schema.Type.STRUCT, LogicalToSqlConverter::toSqlStruct)
        .put(Schema.Type.BYTES, LogicalToSqlConverter::handleBytes)
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

    private static Type handleBytes(final Schema schema) {
      DecimalUtil.requireDecimal(schema);
      return Decimal.of(schema);
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

  private static final class LogicalFromSqlConverter implements SqlToLogicalTypeConverter {

    private static final Map<SqlType, Function<Type, SchemaBuilder>> SQL_TO_LOGICAL =
        ImmutableMap.<SqlType, Function<Type, SchemaBuilder>>builder()
        .put(SqlType.STRING, t -> SchemaBuilder.string().optional())
        .put(SqlType.BOOLEAN, t -> SchemaBuilder.bool().optional())
        .put(SqlType.INTEGER, t -> SchemaBuilder.int32().optional())
        .put(SqlType.BIGINT, t -> SchemaBuilder.int64().optional())
        .put(SqlType.DOUBLE, t -> SchemaBuilder.float64().optional())
        .put(SqlType.DECIMAL, t -> LogicalFromSqlConverter.fromSqlDecimal((Decimal) t))
        .put(SqlType.ARRAY, t -> LogicalFromSqlConverter.fromSqlArray((Array) t))
        .put(SqlType.MAP, t -> LogicalFromSqlConverter
            .fromSqlMap((io.confluent.ksql.parser.tree.Map) t))
        .put(SqlType.STRUCT, t -> LogicalFromSqlConverter.fromSqlStruct((Struct) t))
        .build();

    @Override
    public Schema fromSqlType(final Type sqlType) {
      return logicalType(sqlType).build();
    }

    @Override
    public Schema fromSqlType(final Type type, final String name, final String doc) {
      return logicalType(type).name(name).doc(doc).build();
    }

    private static SchemaBuilder logicalType(final Type sqlType) {
      final Function<Type, SchemaBuilder> handler = SQL_TO_LOGICAL.get(sqlType.getSqlType());
      if (handler == null) {
        throw new KsqlException("Unexpected sql type: " + sqlType);
      }

      return handler.apply(sqlType);
    }

    private static SchemaBuilder fromSqlDecimal(final Decimal sqlType) {
      return DecimalUtil.builder(sqlType.getPrecision(), sqlType.getScale());
    }

    private static SchemaBuilder fromSqlArray(final Array sqlType) {
      return SchemaBuilder
          .array(logicalType(sqlType.getItemType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlMap(final io.confluent.ksql.parser.tree.Map sqlType) {
      return SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, logicalType(sqlType.getValueType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlStruct(final Struct struct) {
      final SchemaBuilder builder = SchemaBuilder.struct();

      struct.getFields()
          .forEach(field -> builder.field(field.getName(), logicalType(field.getType()).build()));

      return builder
          .optional();
    }
  }

  private static class JavaToSqlConverter implements JavaToSqlTypeConverter {

    private static final Map<java.lang.reflect.Type, SqlType> JAVA_TO_SQL
        = ImmutableMap.<java.lang.reflect.Type, SqlType>builder()
        .put(Boolean.class, SqlType.BOOLEAN)
        .put(Integer.class, SqlType.INTEGER)
        .put(Long.class, SqlType.BIGINT)
        .put(Double.class, SqlType.DOUBLE)
        .put(String.class, SqlType.STRING)
        // Structured types not required yet.
        .build();

    @Override
    public SqlType toSqlType(final Class<?> javaType) {
      final SqlType sqlType = JAVA_TO_SQL.get(javaType);
      if (sqlType == null) {
        throw new KsqlException("Unexpected java type: " + javaType);
      }

      return sqlType;
    }
  }
}
