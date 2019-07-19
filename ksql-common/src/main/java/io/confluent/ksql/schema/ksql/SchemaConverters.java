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
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Util class for converting between KSQL's {@link LogicalSchema} and its SQL types.
 *
 * <p>KSQL the following main type systems / schema types:
 *
 * <ul>
 *   <li>
 *     <b>{@link SqlType}:</b>
 *     - the SQL type system, e.g. INTEGER, BIGINT, ARRAY&lt;something&gt;, etc
 *   </li>
 *   <li>
 *     <b>{@link LogicalSchema}:</b>
 *     - the set of named {@code SQL} types that represent the schema of one row in a KSQL stream
 *     or table.
 *   </li>
 *   <li>
 *     <b>PhysicalSchema</b>
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
    SqlType toSqlType(Schema schema);
  }

  public interface SqlToLogicalTypeConverter {

    /**
     * @see #fromSqlType(SqlType, String, String)
     */
    Schema fromSqlType(SqlType sqlType);

    /**
     * Convert the supplied primitive {@code sqlType} to its corresponding logical KSQL schema.
     *
     * @param sqlType the sql type to convert
     * @param name    the name for the schema
     * @param doc     the doc for the schema
     * @return the logical schema.
     */
    Schema fromSqlType(SqlType sqlType, String name, String doc);
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
    SqlBaseType toSqlType(Class<?> javaType);
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

    private static final Map<Schema.Type, Function<Schema, SqlType>> LOGICAL_TO_SQL = ImmutableMap
        .<Schema.Type, Function<Schema, SqlType>>builder()
        .put(Schema.Type.INT32, s -> SqlTypes.INTEGER)
        .put(Schema.Type.INT64, s -> SqlTypes.BIGINT)
        .put(Schema.Type.FLOAT64, s -> SqlTypes.DOUBLE)
        .put(Schema.Type.BOOLEAN, s -> SqlTypes.BOOLEAN)
        .put(Schema.Type.STRING, s -> SqlTypes.STRING)
        .put(Schema.Type.ARRAY, LogicalToSqlConverter::toSqlArray)
        .put(Schema.Type.MAP, LogicalToSqlConverter::toSqlMap)
        .put(Schema.Type.STRUCT, LogicalToSqlConverter::toSqlStruct)
        .put(Schema.Type.BYTES, LogicalToSqlConverter::handleBytes)
        .build();

    @Override
    public SqlType toSqlType(final Schema schema) {
      return sqlType(schema);
    }

    private static SqlType sqlType(final Schema schema) {
      final Function<Schema, SqlType> handler = LOGICAL_TO_SQL.get(schema.type());
      if (handler == null) {
        throw new KsqlException("Unexpected logical type: " + schema);
      }

      return handler.apply(schema);
    }

    private static SqlDecimal handleBytes(final Schema schema) {
      DecimalUtil.requireDecimal(schema);
      return SqlDecimal.of(DecimalUtil.precision(schema), DecimalUtil.scale(schema));
    }

    private static SqlArray toSqlArray(final Schema schema) {
      return SqlArray.of(sqlType(schema.valueSchema()));
    }

    private static SqlMap toSqlMap(final Schema schema) {
      if (schema.keySchema().type() != Schema.Type.STRING) {
        throw new KsqlException("Unsupported map key type: " + schema.keySchema());
      }
      return SqlMap.of(sqlType(schema.valueSchema()));
    }

    private static SqlStruct toSqlStruct(final Schema schema) {
      final SqlStruct.Builder builder = SqlStruct.builder();

      schema.schema().fields()
          .forEach(field -> builder.field(field.name(), sqlType(field.schema())));

      return builder.build();
    }
  }

  private static final class LogicalFromSqlConverter implements SqlToLogicalTypeConverter {

    private static final Map<SqlBaseType, Function<SqlType, SchemaBuilder>> SQL_TO_LOGICAL =
        ImmutableMap.<SqlBaseType, Function<SqlType, SchemaBuilder>>builder()
            .put(SqlBaseType.STRING, t -> SchemaBuilder.string().optional())
            .put(SqlBaseType.BOOLEAN, t -> SchemaBuilder.bool().optional())
            .put(SqlBaseType.INTEGER, t -> SchemaBuilder.int32().optional())
            .put(SqlBaseType.BIGINT, t -> SchemaBuilder.int64().optional())
            .put(SqlBaseType.DOUBLE, t -> SchemaBuilder.float64().optional())
            .put(SqlBaseType.DECIMAL, t -> LogicalFromSqlConverter.fromSqlDecimal((SqlDecimal) t))
            .put(SqlBaseType.ARRAY, t -> LogicalFromSqlConverter.fromSqlArray((SqlArray) t))
            .put(SqlBaseType.MAP, t -> LogicalFromSqlConverter.fromSqlMap((SqlMap) t))
            .put(SqlBaseType.STRUCT, t -> LogicalFromSqlConverter.fromSqlStruct((SqlStruct) t))
        .build();

    @Override
    public Schema fromSqlType(final SqlType type) {
      return logicalType(type).build();
    }

    @Override
    public Schema fromSqlType(final SqlType type, final String name, final String doc) {
      return logicalType(type).name(name).doc(doc).build();
    }

    private static SchemaBuilder logicalType(final SqlType sqlType) {
      final Function<SqlType, SchemaBuilder> handler = SQL_TO_LOGICAL.get(sqlType.baseType());
      if (handler == null) {
        throw new KsqlException("Unexpected sql type: " + sqlType);
      }

      return handler.apply(sqlType);
    }

    private static SchemaBuilder fromSqlDecimal(final SqlDecimal sqlDecimal) {
      return DecimalUtil.builder(sqlDecimal.getPrecision(), sqlDecimal.getScale());
    }

    private static SchemaBuilder fromSqlArray(final SqlArray sqlArray) {
      return SchemaBuilder
          .array(logicalType(sqlArray.getItemType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlMap(final SqlMap sqlMap) {
      return SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, logicalType(sqlMap.getValueType()).build())
          .optional();
    }

    private static SchemaBuilder fromSqlStruct(final SqlStruct struct) {
      final SchemaBuilder builder = SchemaBuilder.struct();

      struct.getFields()
          .forEach(field -> builder.field(field.getName(), logicalType(field.getType()).build()));

      return builder
          .optional();
    }
  }

  private static class JavaToSqlConverter implements JavaToSqlTypeConverter {

    private static final Map<java.lang.reflect.Type, SqlBaseType> JAVA_TO_SQL
        = ImmutableMap.<java.lang.reflect.Type, SqlBaseType>builder()
        .put(Boolean.class, SqlBaseType.BOOLEAN)
        .put(Integer.class, SqlBaseType.INTEGER)
        .put(Long.class, SqlBaseType.BIGINT)
        .put(Double.class, SqlBaseType.DOUBLE)
        .put(String.class, SqlBaseType.STRING)
        // Structured types not required yet.
        .build();

    @Override
    public SqlBaseType toSqlType(final Class<?> javaType) {
      final SqlBaseType sqlType = JAVA_TO_SQL.get(javaType);
      if (sqlType == null) {
        throw new KsqlException("Unexpected java type: " + javaType);
      }

      return sqlType;
    }
  }
}
